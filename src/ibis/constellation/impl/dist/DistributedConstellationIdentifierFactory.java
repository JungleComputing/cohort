package ibis.constellation.impl.dist;

import ibis.constellation.ConstellationIdentifier;
import ibis.constellation.extra.ConstellationIdentifierFactory;

public class DistributedConstellationIdentifierFactory implements ConstellationIdentifierFactory {

    private final long rank;
    private int count; 
       
    DistributedConstellationIdentifierFactory(long rank) { 
        this.rank = rank << 32;
    }
    
    public synchronized ConstellationIdentifier generateConstellationIdentifier() {
        return new ConstellationIdentifier(rank | count++);
    }
}

