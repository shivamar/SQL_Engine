package edu.buffalo.cse562.BDBIndexers;

public class KeyClass {
	private long key = 0;		
	
	public KeyClass(long k){
		setKey(k);
	}

	public long getKey() {
		return key;
	}

	public void setKey(long key) {
		this.key = key;
	}
}
