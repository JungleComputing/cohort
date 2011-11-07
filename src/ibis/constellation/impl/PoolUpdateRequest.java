package ibis.constellation.impl;

import java.io.Serializable;

public class PoolUpdateRequest implements Serializable { 
	private static final long serialVersionUID = -4258898100133094472L;

	final String tag; 
	final long timestamp;
	
	PoolUpdateRequest(String tag, long timestamp) { 
		this.tag = tag;
		this.timestamp = timestamp;
	}
}