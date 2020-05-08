package keyValueStore.client;

import java.net.*;

import keyValueStore.util.FileProcessor;

public class ClientHandler {
	
	private String coord = null;
	static int port_number = 0;
	static String ip_addr = null;
	static Socket soc = null;
	
	public ClientHandler(String c) {
		coord = c;
	}
	
	public void readFile(FileProcessor fp) {
		
		while(true){

			String temp = fp.readLine();
			String[] arr;

			if(temp == null){
				break;
			} 	
			
			arr = temp.split(" ");
			
			if(coord.equalsIgnoreCase(arr[0])) 
			{
				ip_addr = arr[1];
				port_number = Integer.parseInt(arr[2]);
			}		
		}

		System.out.println("Coordinator: "  + ip_addr + " " + port_number);
	}
		
}
