package src.server;

import java.util.*;

public class ReadRepair {
	
	private HashMap<String,Boolean> sev = new HashMap<String,Boolean>();
		public HashMap<String,Long> serversTimestamp = new HashMap<String,Long>();


	private int id = 0;
	private String value = null;
	private int key = 0;
	private Boolean readstat = false;
	private Boolean rrstatus = false;
	private long timestamp = 0;
	
	public ReadRepair(int ids, int keys, String values, long times) {
		value = values;
		id = ids;
		key = keys;
		timestamp = times;		
	}

	public void setTimestamp(long t) {
		this.timestamp = t;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setValue(String v) {
		this.value = v;
	}

	public String getValue() {
		return value;
	}

	public void setKey(int k) {
		this.key = k;
	}
	
	public int getKey() {
		return key;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public HashMap<String,Boolean> getServers() {
		return sev;
	}

	public void addServerList(String serverName, Boolean b) {
		sev.put(serverName, b);
	}

	public void updateServerList() {
		for(String name: sev.keySet()) {
			sev.replace(name, false);
		}
	}
	
	public Boolean checkConsLvl(int consistencyIn) {
		int v = 0;
		for(String name: serversTimestamp.keySet()) {
			if(serversTimestamp.get(name) == timestamp) {
				v++;
				if(v == consistencyIn)
					return true;
			}
		}
		return false;
	}

	public Boolean getReadStatus() {
		return readstat;
	}

	public void setReadStatus(Boolean readstat) {
		this.readstat = readstat;
	}

	public void setReadRepairFlag(Boolean statusIn) {
		rrstatus = statusIn;
	}

	public Boolean getReadRepairFlag() {
		return rrstatus;
	}

}
