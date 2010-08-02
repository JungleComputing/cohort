package ibis.cohort.impl.distributed;

import ibis.cohort.CohortIdentifier;
import ibis.cohort.Context;

public class StealRequest extends Message {
    
    private static final long serialVersionUID = 2655647847327367590L;
   
    public final Context context;
    
    // Note allowRestricted is set to false when the StealRequest traverses the 
    // network.
    private transient boolean allowRestricted;  
    
    public StealRequest(final CohortIdentifier source, final Context context) {  
        // Use this for a remote steal request;
        super(source);
        this.context = context;
        allowRestricted = true;
    }
   
    @Override
    public boolean requiresRandomSelection() {
        return true;
    }
    
    public void setAllowRestricted() { 
        allowRestricted = false;
    }
    
    public boolean allowRestricted() { 
        return allowRestricted;
    }
    
    
}
