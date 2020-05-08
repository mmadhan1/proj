package src.server;

import java.io.*;
import java.net.*;
import java.util.*;

import src.keyValue.KeyValue;
import src.util.FileProcessor;

public class ServerHandler {
	
	private HashMap<String,Boolean> connectedServers = new HashMap<String,Boolean>();
	
	static HashMap<Integer,KeyValue.KeyValuePair> store = new HashMap<Integer,KeyValue.KeyValuePair>();
	static HashMap<String,String> s_ip = new HashMap<String,String>();
	static HashMap<String,Integer> s_port = new HashMap<String,Integer>();	
	private String serverName = null;
	static ServerSocket server;
	static int port = 0;
	
	public ServerHandler(String n, int p) {
		setName(n);
		port = p;
	}

	public String getName() {
		return serverName;
	}

	public void setName(String n) {
		this.serverName = n;
	}

	public void printOut() {
		
		for(int key: store.keySet()) {
			System.out.println("Key: "+store.get(key).getKey() + "Value: " + store.get(key).getValue() + "Unique: " + store.get(key).getTime());
		}
	}

	public synchronized boolean getServerStatus(String nameIn) {
		return connectedServers.get(nameIn);
	}
	
	
	public synchronized void addOnlineServers(String serverNameIn, Boolean statusIn) {
		
		if(connectedServers.containsKey(serverNameIn)) 
			connectedServers.replace(serverNameIn, statusIn);
		else 
			connectedServers.put(serverNameIn, statusIn);
	}
	

	public synchronized int countOnlineServers() {
		int v = 0;
		for(String name: connectedServers.keySet()) 
			if(connectedServers.get(name) == true)
				v++;	
		return v;
	}
	
	public void readFile(FileProcessor fp) {
		
		while(true){
			String value = fp.readLine();
			if(value == null){
				break;	
			}
			String[] splitValue;
			splitValue = value.split(" ");
			s_ip.put(splitValue[0], splitValue[1]);
			s_port.put(splitValue[0], Integer.parseInt(splitValue[2]));		
		}
	}

	public void readLog(FileProcessor fp) {
		
		while(true) {

			String line = fp.readLine();
			
			if(line == null) {
				break;
			}
			
			String [] splitValue=line.split(" ");
			int key = Integer.parseInt(splitValue[0]);
			
			KeyValue.KeyValuePair.Builder k_store = KeyValue.KeyValuePair.newBuilder();			
			k_store.setKey(key);
			k_store.setValue(splitValue[1]);
			k_store.setTime(Long.parseLong(splitValue[2]));
			
			if(store.containsKey(key)) 
				store.replace(key, k_store.build());
			else{ 
				store.put(key, k_store.build());
			}
		}
	}	
	
}
