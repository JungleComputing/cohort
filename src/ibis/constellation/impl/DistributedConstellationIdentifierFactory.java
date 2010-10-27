package ibis.constellation.impl;

import ibis.constellation.ConstellationIdentifier;
import ibis.constellation.extra.ConstellationIdentifierFactory;

public class DistributedConstellationIdentifierFactory implements ConstellationIdentifierFactory {

	private final long rank;
    private final long mask;
    private int count; 
       
    DistributedConstellationIdentifierFactory(long rank) { 
        this.rank = rank;
        this.mask = rank << 32;
    }
    
    public synchronized ConstellationIdentifier generateConstellationIdentifier() {
        return new ConstellationIdentifier(mask | count++);
    }
    
    public boolean isLocal(ConstellationIdentifier cid) { 
    	
    	/*
    	boolean local = ((cid.id >> 32) & 0xffffffff) == rank; 
    	
    	if (!local) { 
    		System.out.println("!local -"  + cid + "- " + ((cid.id >> 32) & 0xffffffff) + " " + rank);
    	}
    	
    	return local;
    	*/
    	
    	return ((cid.id >> 32) & 0xffffffff) == rank;
    }
}

