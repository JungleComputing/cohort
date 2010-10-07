package ibis.constellation.impl.dist;

import ibis.constellation.ConstellationIdentifier;
import ibis.ipl.IbisIdentifier;

public class DistributedConstellationIdentifier extends ConstellationIdentifier {

    private static final long serialVersionUID = -1145931224872635119L;
  
    private final IbisIdentifier ibis;
   
    public DistributedConstellationIdentifier(final IbisIdentifier ibis, 
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
