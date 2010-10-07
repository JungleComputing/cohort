package ibis.constellation.impl.distributed.dist;

import ibis.constellation.CohortIdentifier;
import ibis.constellation.extra.CohortIdentifierFactory;
import ibis.ipl.IbisIdentifier;

public class DistributedCohortIdentifierFactory implements CohortIdentifierFactory {

    private final IbisIdentifier ibis;
    private final long rank;
    private int count; 
       
    DistributedCohortIdentifierFactory(IbisIdentifier ibis, long rank) { 
        this.ibis = ibis;
        this.rank = rank;
    }
    
    public synchronized CohortIdentifier generateCohortIdentifier() {
        return new DistributedCohortIdentifier(ibis, rank, count++);
    }
}

