package ibis.constellation.impl.distributed;

import ibis.constellation.CohortIdentifier;
import ibis.constellation.StealPool;
import ibis.constellation.WorkerContext;

public class StealRequest extends Message {
    
    private static final long serialVersionUID = 2655647847327367590L;
   
    public final WorkerContext context;
    public final StealPool pool;
    
    // Note allowRestricted is set to false when the StealRequest traverses the 
    // network.
    private transient boolean allowRestricted;  
    
    public StealRequest(final CohortIdentifier source, final WorkerContext context, final StealPool pool) {  
        // Use this for a remote steal request;
        super(source);
        this.context = context;
        this.pool = pool;
        allowRestricted = true;
    }
   
    public StealRequest(final CohortIdentifier source, final WorkerContext context) {  
        this(source, context, StealPool.WORLD);
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
