package src.server;

import java.io.*;
import java.net.*;

import src.keyValue.KeyValue;
import src.util.FileProcessor;
import src.util.writeLog;

public class ReplicaServers{
	
	public static void main(String[] args){
		
		if(args.length != 3){
			System.out.println("Use: ./server.sh server_name port file>\n");
			System.exit(0);
		}
				
		ServerHandler sh = new ServerHandler(args[0],Integer.parseInt(args[1]));
		FileProcessor fp = new FileProcessor(args[2]);
		
		sh.readFile(fp);
		fp.close();
		
		String path = "log/" + sh.getName() +".log";			
		FileProcessor readLog = new FileProcessor(path);
		
		if(readLog.isReadable()) {
		
			sh.readLog(readLog);
			readLog.close();
			sh.printOut();
		}
		
		writeLog wrlog = new writeLog(path);
		
		try {
			ServerHandler.server = new ServerSocket(ServerHandler.port);
		}
		catch(IOException i) {
			System.out.println(i);
		}
	
		try {
			System.out.println("Running on " + InetAddress.getLocalHost().getHostAddress() +" " + + ServerHandler.port);
		} 
		catch(UnknownHostException e1) {
			e1.printStackTrace();
		}
		
		new Thread(new Runnable() {

			@Override
			public void run() {
				while(true) {

					KeyValue.KeyValueMessage.Builder km = KeyValue.KeyValueMessage.newBuilder();
					km.setServerName(sh.getName());
					km.setConnection(0);
					
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					
					for(String serverName : ServerHandler.s_ip.keySet()) {
						try {

							Socket socket = new Socket(ServerHandler.s_ip.get(serverName), ServerHandler.s_port.get(serverName));
							OutputStream out = socket.getOutputStream();
							
							km.build().writeDelimitedTo(out);
							
							sh.addOnlineServers(serverName, true);
							
							out.flush();
							out.close();
							socket.close();
												
						} catch(ConnectException e) {
			
							sh.addOnlineServers(serverName, false);
						} catch(UnknownHostException e) {
							e.printStackTrace();
						} catch(IOException e) {
							sh.addOnlineServers(serverName, false);
							e.printStackTrace();
						}
					
					}
	
				}
			}
		}).start();
		
		Socket request = null;
		
		while(true) {
			try {
				
				request = ServerHandler.server.accept();
				
				InputStream in = request.getInputStream();
				KeyValue.KeyValueMessage keyValueMsg = KeyValue.KeyValueMessage.parseDelimitedFrom(in);
				

				if(keyValueMsg.getConnection() == 1) {
					
					System.out.println("got message from Client");
					Thread coordinatorThread = new Thread(new Coordinator(request, sh, keyValueMsg));
					coordinatorThread.start();
					
				}
				
				if(keyValueMsg.getConnection() == 0){
					
					String receiveServer = keyValueMsg.getServerName();
					
					KeyValue.KeyValueMessage.Builder keyMessage = KeyValue.KeyValueMessage.newBuilder();
					OutputStream out = null;
					
					if(keyValueMsg.hasPutKey()) {
						
						KeyValue.Put put = keyValueMsg.getPutKey();
						KeyValue.KeyValuePair keyStore = put.getKeyval();

						String writeAheadLog = keyStore.getKey() + " " + keyStore.getValue() + " " + keyStore.getTime();
						wrlog.writeToFile(writeAheadLog);						
						
						ServerHandler.store.put(keyStore.getKey(), keyStore);							
						System.out.println("message stored\n");
						sh.printOut();
							
						KeyValue.WriteResponse.Builder wr = KeyValue.WriteResponse.newBuilder();
						
						wr.setWriteReply(true);
						wr.setKey(keyStore.getKey());
						wr.setId(put.getId());
						
						out = request.getOutputStream();
						keyMessage.setWriteResponse(wr.build());
						keyMessage.build().writeDelimitedTo(out);
						
						out.flush();
						out.close();
						
					}
					
					if(keyValueMsg.hasReadRepair()) {
						
						KeyValue.ReadRepair rr = keyValueMsg.getReadRepair();
						KeyValue.KeyValuePair keyStore = rr.getKeyval();
						int key = keyStore.getKey();
						
						String writeAheadLog = keyStore.getKey() + " " + keyStore.getValue() + " " + keyStore.getTime();
						wrlog.writeToFile(writeAheadLog);
						
						ServerHandler.store.put(key, keyStore);						
						System.out.println("Read Repair done completed");
						
						sh.printOut();
					}
					
					if(keyValueMsg.hasGetKey()) {
						
						int key = keyValueMsg.getGetKey().getKey();
						KeyValue.KeyValuePair keyStore = null;
						KeyValue.ReadResponse.Builder readResp = KeyValue.ReadResponse.newBuilder();
						
						if(ServerHandler.store.containsKey(key)) {
							
							keyStore = ServerHandler.store.get(key);
							readResp.setReadStatus(true);
							readResp.setKeyval(keyStore);
							readResp.setId(keyValueMsg.getGetKey().getId());
							
						}					
						else {
							KeyValue.KeyValuePair.Builder ks = KeyValue.KeyValuePair.newBuilder();
							ks.setKey(key);

							readResp.setReadStatus(false);
							readResp.setKeyval(ks);
							readResp.setId(keyValueMsg.getGetKey().getId());
						}
						
						System.out.println("response sent");
						out = request.getOutputStream();
						keyMessage.setReadResponse(readResp);
						keyMessage.build().writeDelimitedTo(out);
						
						out.flush();
						out.close();
						
					}	
			
					in.close();
					request.close();	
				}
			
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
}
