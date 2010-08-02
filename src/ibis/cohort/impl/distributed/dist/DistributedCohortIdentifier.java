package ibis.cohort.impl.distributed.dist;

import ibis.cohort.CohortIdentifier;
import ibis.ipl.IbisIdentifier;

public class DistributedCohortIdentifier extends CohortIdentifier {

    private static final long serialVersionUID = -1145931224872635119L;
  
    private final IbisIdentifier ibis;
   
    public DistributedCohortIdentifier(final IbisIdentifier ibis, 
            final long rank, final int workerID) {
        super(rank << 32 | workerID);
        this.ibis = ibis;
    }
    
    protected IbisIdentifier getIbis() { 
        return ibis;
    }
    
    public String toString() { 
        return super.toString() + " on " + ibis.toString();
    }
    
}
