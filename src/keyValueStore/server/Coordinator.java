package keyValueStore.server;

import java.io.*;
import java.util.*;
import java.net.*;

import keyValueStore.keyValue.KeyValue;

public class Coordinator implements Runnable{
	
	private HashMap<Integer, Integer> r_resp = new HashMap<Integer,Integer>();
	private HashMap<Integer, Integer> w_resp = new HashMap<Integer,Integer>();
	private HashMap<Integer, Integer> reply_store = new HashMap<Integer,Integer>();
	private HashMap<Integer,Integer> consist_table = new HashMap<Integer,Integer>();
	private HashMap<Integer,ReadRepair> r_repair = new HashMap<Integer,ReadRepair>();

	private KeyValue.KeyValueMessage keyValueMsg = null;
	private Socket c_sock = null;
	private ServerHandler sh = null;

	public Coordinator(Socket in, ServerHandler scIn, KeyValue.KeyValueMessage msgIn) {

		c_sock = in;
		sh = scIn;
		keyValueMsg = msgIn;
		System.out.println("Coordinator starts");
	}

	@Override
	public void run() {
		
		if(keyValueMsg != null) {
			clientApp(keyValueMsg);
		}
		
		while(true) {
			try {

				InputStream in = c_sock.getInputStream();
				KeyValue.KeyValueMessage incomingMsg = KeyValue.KeyValueMessage.parseDelimitedFrom(in);
				
				if(incomingMsg != null) {
					clientApp(incomingMsg);
				}
			
			} catch (IOException e) {
				e.printStackTrace();
			}						
		}
	}

	private synchronized void serverApp(String serverName, KeyValue.KeyValueMessage responseMsg) throws IOException {
		
		if(responseMsg.hasWriteResponse()) {

			KeyValue.WriteResponse wr = responseMsg.getWriteResponse();
			int id = wr.getId();
			if(wr.getWriteReply()) {
			
				int cVal = w_resp.get(id);
				w_resp.replace(id,cVal+1);
			}
			

			if(2 == w_resp.get(id)) {
				
				System.out.println("Key: " + wr.getKey() + " Status: " + wr.getWriteReply());
				consist_table.replace(id, -1);
				
				KeyValue.KeyValueMessage.Builder responseClient = KeyValue.KeyValueMessage.newBuilder();
				responseClient.setWriteResponse(wr);
				
				try {

					OutputStream out = c_sock.getOutputStream();
					responseClient.build().writeDelimitedTo(out);
					out.flush();
					
				} catch(IOException i) {
					System.out.println("Client cannot be reached");

				}
			}
		}
		
		if(responseMsg.hasReadResponse()) {
		
			KeyValue.ReadResponse rr = responseMsg.getReadResponse();
			int id = rr.getId();
			
			int replies = reply_store.get(id);
			reply_store.replace(id, replies+1);
			
			if(rr.getReadStatus()) {
				
				long time = rr.getKeyval().getTime();
				String value = rr.getKeyval().getValue();
				int key = rr.getKeyval().getKey();
			
				if(!r_repair.containsKey(id)) {			
					
					ReadRepair r = new ReadRepair(id, key, value, time);
					r.addServerList(serverName, true);
					
					r_repair.put(id, r);
				}

				r_repair.get(id).setReadStatus(true);
				r_repair.get(id).serversTimestamp.put(serverName, time);
				
				if(time > r_repair.get(id).getTimestamp()) {

					r_repair.get(id).setId(id);
					r_repair.get(id).setKey(key);
					r_repair.get(id).setValue(value);
					r_repair.get(id).setTimestamp(time);
					r_repair.get(id).updateServerList();
					r_repair.get(id).setReadRepairFlag(true);
					r_repair.get(id).addServerList(serverName, true);
				}
				
				if(time < r_repair.get(id).getTimestamp()) {

					r_repair.get(id).addServerList(serverName, false);
					r_repair.get(id).setReadRepairFlag(true);
				}
				
				int cVal = r_resp.get(id);
				r_resp.replace(id, cVal+1);
				
				if(r_resp.get(id) == consist_table.get(id)) {

					consist_table.replace(id, -1);
					
					KeyValue.KeyValueMessage.Builder keyMessage = KeyValue.KeyValueMessage.newBuilder();
					KeyValue.KeyValuePair.Builder keyStore = KeyValue.KeyValuePair.newBuilder();
					KeyValue.ReadResponse.Builder readResponse = KeyValue.ReadResponse.newBuilder();				
					
					keyStore.setKey(r_repair.get(id).getKey());
					keyStore.setValue(r_repair.get(id).getValue());		
					keyStore.setTime(r_repair.get(id).getTimestamp());
					
					readResponse.setKeyval(keyStore.build());
					readResponse.setId(r_repair.get(id).getId());
					readResponse.setReadStatus(r_repair.get(id).getReadStatus());
					
					keyMessage.setReadResponse(readResponse.build());

					try {

						OutputStream out = c_sock.getOutputStream();
						keyMessage.build().writeDelimitedTo(out);
						out.flush();
					
					} catch(IOException i) {
						System.out.println("Client cannot be reached");
						
					}			
				}
				
			}
			else {
				int key = rr.getKeyval().getKey();
				
				if(!r_repair.containsKey(id)) {
					
					ReadRepair r = new ReadRepair(id, key, null, 0);
					r.addServerList(serverName, false);
					r_repair.put(id,r);			
				}
				
				r_repair.get(id).addServerList(serverName, false);
				
			}
											

			if(reply_store.get(id) == sh.countOnlineServers()) {

				if(consist_table.get(id) != -1 && r_repair.get(id).checkConsLvl(consist_table.get(id)) == false) {

					KeyValue.Exception.Builder exc = KeyValue.Exception.newBuilder();
					KeyValue.KeyValueMessage.Builder keyMessage = KeyValue.KeyValueMessage.newBuilder();
					
					exc.setKey(r_repair.get(id).getKey());
					exc.setExceptionMessage("Consistency was not fulfilled");
					exc.setMethod("GET");
					
					keyMessage.setException(exc.build());
					
					try {

						OutputStream out = c_sock.getOutputStream();
						keyMessage.build().writeDelimitedTo(out);
						out.flush();
						
					} catch(IOException i) {

						System.out.println("Client cannot be reached");
						i.printStackTrace();
					}
					
				}

				else if(r_repair.get(id).getReadStatus() == false){

					KeyValue.KeyValuePair.Builder keyStore = KeyValue.KeyValuePair.newBuilder();
				  	KeyValue.KeyValueMessage.Builder keyMessage = KeyValue.KeyValueMessage.newBuilder();
					KeyValue.ReadResponse.Builder readResponse = KeyValue.ReadResponse.newBuilder();				
					
					keyStore.setKey(r_repair.get(id).getKey());
					
					readResponse.setKeyval(keyStore.build());
					readResponse.setId(r_repair.get(id).getId());
					readResponse.setReadStatus(r_repair.get(id).getReadStatus());
					
					keyMessage.setReadResponse(readResponse.build());

					try {

						OutputStream out = c_sock.getOutputStream();
						keyMessage.build().writeDelimitedTo(out);
						out.flush();

					} catch(IOException i) {
						System.out.println("Client cannot be reached");
				
					}				
			  }
				
				readRepair_background(serverName, id);
			 
			}
		}
	}

	private void clientApp(KeyValue.KeyValueMessage incomingMsg) {
		
		KeyValue.KeyValueMessage.Builder keyValueBuilder = null;
		
		if(incomingMsg.hasPutKey()) {
			
			Date date = new Date();
			long time = date.getTime();

			KeyValue.Put.Builder putServer = KeyValue.Put.newBuilder();		
			KeyValue.KeyValuePair.Builder keyStore = KeyValue.KeyValuePair.newBuilder();

			KeyValue.Put putMessage = incomingMsg.getPutKey();
			int consistency = putMessage.getConsistency();
			
			keyStore.setTime(time);
			keyStore.setKey(putMessage.getKeyval().getKey());
			keyStore.setValue(putMessage.getKeyval().getValue());				
						
			putServer.setId(putMessage.getId());
			putServer.setKeyval(keyStore.build());
			putServer.setConsistency(consistency);
			
			consist_table.put(putServer.getId(),consistency);
			w_resp.put(putServer.getId(), 0);
			
			keyValueBuilder = KeyValue.KeyValueMessage.newBuilder();
			keyValueBuilder.setConnection(0);

			keyValueBuilder.setPutKey(putServer.build());
			

			if(sh.countOnlineServers() < consistency) {

				KeyValue.KeyValueMessage.Builder keyMessage = KeyValue.KeyValueMessage.newBuilder();
				KeyValue.Exception.Builder exc = KeyValue.Exception.newBuilder();
				
				exc.setKey(putServer.getKeyval().getKey());
				exc.setMethod("PUT");
				exc.setExceptionMessage("Not enough servers are online");
				
				keyMessage.setException(exc.build());
				
				try {
					OutputStream out = c_sock.getOutputStream();
					keyMessage.build().writeDelimitedTo(out);
					out.flush();
					
				} catch(IOException i) {

					System.out.println("Client cannot be reached");
					i.printStackTrace();
				}
				
			}else {
				serverSend(keyValueBuilder);
			}
		}
		
		if(incomingMsg.hasGetKey()) {
			
			KeyValue.Get getMessage = incomingMsg.getGetKey();
			int consistency = getMessage.getConsistency();
			
			KeyValue.Get.Builder getServer = KeyValue.Get.newBuilder();	
			

			getServer.setId(getMessage.getId());
			getServer.setKey(getMessage.getKey());
			getServer.setConsistency(getMessage.getConsistency());
				
			consist_table.put(getMessage.getId(), consistency);
			r_resp.put(getMessage.getId(), 0);
			reply_store.put(getMessage.getId(), 0);
			
			keyValueBuilder = KeyValue.KeyValueMessage.newBuilder();
			keyValueBuilder.setConnection(0);
			keyValueBuilder.setGetKey(getServer.build());
		
			if(sh.countOnlineServers() < consistency) {


				KeyValue.KeyValueMessage.Builder keyMessage = KeyValue.KeyValueMessage.newBuilder();
				KeyValue.Exception.Builder exc = KeyValue.Exception.newBuilder();
				
				exc.setKey(getServer.getKey());
				exc.setMethod("GET");
				exc.setExceptionMessage("Not enough servers are online");
				
				keyMessage.setException(exc.build());
				
				try {

					OutputStream out = c_sock.getOutputStream();
					keyMessage.build().writeDelimitedTo(out);
					out.flush();
					
				} catch(IOException i) {

					System.out.println("Client cannot be reached");
					i.printStackTrace();
				}
				
			}else {

				serverSend(keyValueBuilder);
			}
		}
	
	}

private void readRepair_background(String serverName, int id) {
	
			if(r_repair.get(id).getReadStatus() == true) {
				
				HashMap<String,Boolean> list = r_repair.get(id).getServers();
				
				KeyValue.KeyValueMessage.Builder keyMessage = KeyValue.KeyValueMessage.newBuilder();
				KeyValue.KeyValuePair.Builder keyStore = KeyValue.KeyValuePair.newBuilder();
			    KeyValue.ReadRepair.Builder readRepairMsg = KeyValue.ReadRepair.newBuilder();						
				
				keyStore.setKey(r_repair.get(id).getKey());
				keyStore.setValue(r_repair.get(id).getValue());		
				keyStore.setTime(r_repair.get(id).getTimestamp());
				
				readRepairMsg.setKeyval(keyStore.build());
			    readRepairMsg.setId(id);
			    
				keyMessage.setReadRepair(readRepairMsg.build());
				
				for(String name : list.keySet()) {

					if(list.get(name) == false) {
						try {
							
							System.out.println("ReadRepair sending to" + name + "  Key:  " + r_repair.get(id).getKey());
							Socket sock = new Socket(ServerHandler.s_ip.get(name), ServerHandler.s_port.get(name));
							OutputStream out = sock.getOutputStream();
							keyMessage.build().writeDelimitedTo(out);
							out.flush();
							out.close();
							sock.close();
							
						} catch (UnknownHostException e) {
							e.printStackTrace();
						
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
	}

	private void serverSend(KeyValue.KeyValueMessage.Builder keyIn) {
		
		for(String serverName : ServerHandler.s_ip.keySet()) {
			try {

				keyIn.setServerName(sh.getName());
				
				Socket socket = new Socket(ServerHandler.s_ip.get(serverName), ServerHandler.s_port.get(serverName));
				OutputStream out = socket.getOutputStream();
				
				keyIn.build().writeDelimitedTo(out);
				sh.addOnlineServers(serverName, true);
				out.flush();
			
				new Thread(new Runnable(){
					
					public void run() {
						try {

							String server_name = serverName;
							InputStream in = socket.getInputStream();
							KeyValue.KeyValueMessage responseMsg = KeyValue.KeyValueMessage.parseDelimitedFrom(in);
							
							serverApp(server_name,responseMsg);
							
							in.close();
							socket.close();
							
						}catch (IOException e) {
						
							e.printStackTrace();
						}
					}

				}).start();
				
			} catch(ConnectException e) {
				System.out.println(serverName + "not reachable");
		
			} catch (UnknownHostException e) {
				e.printStackTrace();
			
			} catch (IOException e) {
				e.printStackTrace();
			}		
		}
	}	
	
}
