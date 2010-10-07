package ibis.constellation.impl.distributed.dist;

import ibis.constellation.ConstellationIdentifier;
import ibis.constellation.extra.ConstellationIdentifierFactory;
import ibis.ipl.IbisIdentifier;

public class DistributedConstellationIdentifierFactory implements ConstellationIdentifierFactory {

    private final IbisIdentifier ibis;
    private final long rank;
    private int count; 
       
    DistributedConstellationIdentifierFactory(IbisIdentifier ibis, long rank) { 
        this.ibis = ibis;
        this.rank = rank;
    }
    
    public synchronized ConstellationIdentifier generateCohortIdentifier() {
        return new DistributedConstellationIdentifier(ibis, rank, count++);
    }
}

