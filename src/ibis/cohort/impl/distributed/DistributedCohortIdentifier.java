package ibis.cohort.impl.distributed;

import ibis.cohort.CohortIdentifier;
import ibis.ipl.IbisIdentifier;

public class DistributedCohortIdentifier extends CohortIdentifier {

    private static final long serialVersionUID = -1145931224872635119L;
  
    private final IbisIdentifier ibis;
    private final long rank;              // Note: "rank:worker" is a nice human    
    private final int workerID;           //       readable ID!
    
    public DistributedCohortIdentifier(final IbisIdentifier ibis, 
            final long rank, final int workerID) {
        super();
        this.ibis = ibis;
        this.rank = rank;
        this.workerID = workerID;
    }
    
    @Override
    public int hashCode() {
        final int PRIME = 31;
        int result = 1;
        result = PRIME * result + ((ibis == null) ? 0 : ibis.hashCode());
        result = PRIME * result + workerID;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        
        final DistributedCohortIdentifier other = (DistributedCohortIdentifier) obj;
       
        if (workerID != other.workerID)
            return false;
        
        if (ibis == null) {
            return (other.ibis == null); 
        } 
 
        return ibis.equals(other.ibis);
    }
    
    protected IbisIdentifier getIbis() { 
        return ibis;
    }
    
    protected int getWorkerID() { 
        return workerID;
    }
  
    public String simpleName() { 
        return rank + ":" + workerID;
    }
    
    public String toString() { 
        return "Cohort(" + ibis.toString() + " / " + workerID + ")";
    }
    
}
