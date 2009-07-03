package ibis.cohort.impl.multithreaded;

import ibis.cohort.ActivityIdentifier;

public class IDGenerator {

    private final long end;
    
    private long current;

    public IDGenerator(final long start, final long end) {
        super();
        this.current = start;
        this.end = end;
    }
    
    public ActivityIdentifier createActivityID() throws Exception {
        
        if (current >= end) { 
            throw new Exception("Out of identifiers!");
        }
        
        return new Identifier(current++);
    }
}
