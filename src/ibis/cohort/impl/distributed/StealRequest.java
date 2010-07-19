package ibis.cohort.impl.distributed;

import ibis.cohort.CohortIdentifier;
import ibis.cohort.Context;

public class StealRequest extends Message {
    
    private static final long serialVersionUID = 2655647847327367590L;
   
    public final Context context;
      
    public StealRequest(final CohortIdentifier source, final Context context) {  
        // Use this for a remote steal request;
        super(source);
        this.context = context;
    }
   
    @Override
    public boolean requiresRandomSelection() {
        return true;
    }
}
