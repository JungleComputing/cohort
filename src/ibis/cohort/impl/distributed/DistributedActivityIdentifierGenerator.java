package ibis.cohort.impl.distributed;

import ibis.cohort.ActivityIdentifier;

public class DistributedActivityIdentifierGenerator {

    private final DistributedCohortIdentifier cohort;    
    
    private final long end;  
    private long current;

    public DistributedActivityIdentifierGenerator(final DistributedCohortIdentifier cohort, 
            final long start, final long end) {
        super();
        this.cohort = cohort;
        this.current = start;
        this.end = end;
    }
    
    public ActivityIdentifier createActivityID() throws Exception {
        
        if (current >= end) { 
            throw new Exception("Out of identifiers!");
        }
        
        return new DistributedActivityIdentifier(cohort, current++);
    }
}
