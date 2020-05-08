package keyValueStore.util;

import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.File;


public class FileProcessor{

	private	FileReader file = null;
	private BufferedReader fileRead = null;
	private int lineIndex = 0;
	private boolean readable = true;
	

	public FileProcessor(String fileName){
	
		File check = new File(fileName);
		
		if(check.isFile()) {
			try{
				file = new FileReader(fileName);
				fileRead = new BufferedReader(file);
			}
			catch(FileNotFoundException f){
				System.err.println(" file not found - " + fileName);
				System.exit(0); 
			}
		}
		else {
			setReadable(false);
		}
	}


	public String readLine(){
		String currentLine;
		try{
			currentLine = fileRead.readLine();
			if(currentLine == null){
				return null;
			}
			lineIndex++;
			return currentLine;
		}
		catch(IOException i){
			System.out.println(" Read Failed ");
		}
		return null;
	}


    public void close() {
		try{
       	    	fileRead.close();
		}
		catch(IOException i){
			System.err.println("close failed");
         	System.exit(0); 
		}
    }
    

	public boolean isReadable() {
		return readable;
	}
	

	public void setReadable(boolean readable) {
		this.readable = readable;
	}
}
	
