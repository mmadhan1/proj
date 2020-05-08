package src.client;

import java.io.*;
import java.util.*;
import java.net.*;

import src.keyValue.KeyValue;
import src.util.FileProcessor;

public class Client{
		
	public static void main(String [] args) {
					
		ClientHandler ch = new ClientHandler(args[0]);
		FileProcessor fp = new FileProcessor(args[1]);
	    
		Scanner scan = new Scanner(System.in);
		ch.readFile(fp);

		Thread receive = new Thread(new Runnable() {

			@Override
			public void run() {
				while(true) {
					try {
						InputStream in = ClientHandler.soc.getInputStream();
						KeyValue.KeyValueMessage msg_coming = KeyValue.KeyValueMessage.parseDelimitedFrom(in);
						
						if(msg_coming != null) {
					
							System.out.println("\nResponse received");

							if(msg_coming.hasWriteResponse()) {
								KeyValue.WriteResponse wr = msg_coming.getWriteResponse();
								System.out.println("Key: "+wr.getKey() + " Status: " + wr.getWriteReply());	
							}
						
							if(msg_coming.hasReadResponse()) {
								
								KeyValue.ReadResponse readResponse = msg_coming.getReadResponse();
								KeyValue.KeyValuePair keyStore = readResponse.getKeyval();
								
								if(readResponse.getReadStatus()) {
									System.out.println("Key: "+keyStore.getKey() + " Value: " + keyStore.getValue());
								}			
							}
							
							if(msg_coming.hasException()) {
								
								KeyValue.Exception e = msg_coming.getException();
								System.out.println("Error in : " + e.getMethod() + " Method\t" + e.getExceptionMessage());
								
							}
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				
			}			
		});
		
		while(true) {
			String value = scan.next();
	
			KeyValue.KeyValueMessage.Builder kmsg = KeyValue.KeyValueMessage.newBuilder();
			
			try {
			
				if(value.equalsIgnoreCase("put")) {
					String k=scan.next();
					int keyone = Integer.parseInt(k);
					String val = scan.next();
					
					KeyValue.KeyValuePair.Builder keyStore = KeyValue.KeyValuePair.newBuilder();
					keyStore.setKey(keyone);
					keyStore.setValue(val);
					
					KeyValue.Put.Builder putting = KeyValue.Put.newBuilder();
					
					putting.setConsistency(2);
					putting.setKeyval(keyStore.build());
					putting.setId(getUniqueId());
					kmsg.setPutKey(putting.build());
				}
				
				if(value.equalsIgnoreCase("get")) {
					String k=scan.next();
					int keytwo = Integer.parseInt(k);
					KeyValue.Get.Builder getting = KeyValue.Get.newBuilder();
					
					getting.setConsistency(2);
					getting.setKey(keytwo);
					getting.setId(getUniqueId());
					
					kmsg.setGetKey(getting.build());
					
				}
			
			} catch(Exception e) {
				System.out.println("Input is wrong");
				continue;
			}
			try {			

				if(ClientHandler.soc == null) {
					ClientHandler.soc = new Socket(ClientHandler.ip_addr,ClientHandler.port_number);
					kmsg.setConnection(1);
					receive.start();
				}
				
				OutputStream out = ch.soc.getOutputStream();
				kmsg.build().writeDelimitedTo(out);
				out.flush();
				
			} catch(ConnectException e) {
				System.out.println("server error");

			} catch (IOException e) {
				System.out.println("server error");
			}
		}	
	}
	
	public synchronized static int getUniqueId() {
	
		Random r = new Random();
		int id = r.nextInt(Integer.MAX_VALUE);
		return id;
	}
}
