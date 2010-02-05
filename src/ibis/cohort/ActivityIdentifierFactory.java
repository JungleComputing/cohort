package ibis.cohort;

public class ActivityIdentifierFactory {

    private final long high;    
    private final long end;  
    private long current;

    public ActivityIdentifierFactory(
            final long high, final long start, final long end) {
        this.high = high;
        this.current = start;
        this.end = end;
    }
    
    public ActivityIdentifier createActivityID() throws Exception {
        
        if (current >= end) { 
            throw new Exception("Out of identifiers!");
        }
        
        return new ActivityIdentifier(high, current++);
    }
}