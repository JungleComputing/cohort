package ibis.constellation.impl;

import ibis.constellation.ConstellationIdentifier;
import ibis.constellation.StealPool;
import ibis.constellation.WorkerContext;

public class StealRequest extends Message {
    
    private static final long serialVersionUID = 2655647847327367590L;
   
    public final WorkerContext context;
    public final StealPool pool;
    public final int size;
    
    // Note allowRestricted is set to false when the StealRequest traverses the 
    // network.
    private transient boolean allowRestricted;  
    
    public StealRequest(final ConstellationIdentifier source, final WorkerContext context, final StealPool pool, final int size) {  
        // Use this for a remote steal request;
        super(source);
        this.context = context;
        this.pool = pool;
        this.size = size;
        allowRestricted = true;
    }
    
    @Override
    public boolean requiresRandomSelection() {
        return true;
    }
    
    public void doNotAllowRestricted() { 
        allowRestricted = false;
    }
    
    public boolean allowRestricted() { 
        return allowRestricted;
    }
    
    
}
