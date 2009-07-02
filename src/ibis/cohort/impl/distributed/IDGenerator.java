package ibis.cohort.impl.distributed;

import ibis.cohort.ActivityIdentifier;
import ibis.ipl.IbisIdentifier;

public class IDGenerator {

    private final IbisIdentifier ibis;    
    private final int workerID;
    
    private final long end;
    
    private long current;

    public IDGenerator(final IbisIdentifier ibis, final int workerID, 
            final long start, final long end) {
        super();
        this.ibis = ibis;
        this.workerID = workerID;        
        this.current = start;
        this.end = end;
    }
    
    public ActivityIdentifier createActivityID() throws Exception {
        
        if (current >= end) { 
            throw new Exception("Out of identifiers!");
        }
        
        return new Identifier(ibis, workerID, current++);
    }
}
