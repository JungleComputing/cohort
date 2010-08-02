package ibis.cohort.impl.distributed;

import ibis.cohort.ActivityIdentifier;
import ibis.cohort.CohortIdentifier;

public class LookupRequest extends Message {
    
    private static final long serialVersionUID = 2655647847327367590L;
   
    public final ActivityIdentifier missing;
      
    public LookupRequest(final CohortIdentifier source, 
            final ActivityIdentifier missing) {  
        super(source);
        this.missing = missing;
    }

    @Override
    public boolean requiresLookup() {
        return true;
    }
  
    @Override
    public ActivityIdentifier targetActivity() {
        return missing;
    }
    
    
}
