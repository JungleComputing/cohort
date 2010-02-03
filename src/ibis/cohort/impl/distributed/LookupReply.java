package ibis.cohort.impl.distributed;

import ibis.cohort.ActivityIdentifier;
import ibis.cohort.CohortIdentifier;

public class LookupReply extends Message {
    
    private static final long serialVersionUID = 2655647847327367590L;
   
    public final ActivityIdentifier missing;
    public final CohortIdentifier location;
    public final long count;
    
    public LookupReply(
            final CohortIdentifier source,
            final CohortIdentifier target,
            final ActivityIdentifier missing, 
            final CohortIdentifier location, 
            final long count) { 
        
        super(source, target);
        this.missing = missing;
        this.location = location;
        this.count = count;
    }    
}
