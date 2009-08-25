package ibis.cohort.impl.distributed;

import java.io.Serializable;

import ibis.cohort.CohortIdentifier;
import ibis.cohort.Context;

class StealRequest implements Serializable {
    
    private static final long serialVersionUID = 2655647847327367590L;
   
    public final CohortIdentifier src;
    public final Context context;
    
    private transient long timeout = -1;
    private transient boolean local = false; 
    private transient int hops = 0;
    
    public StealRequest(final CohortIdentifier src, final Context context) {  
        super();
        this.src = src;
        this.context = context;
    }
    
    public void setTimeout(long timeout) { 
        this.timeout = timeout;
    }
    
    public long getTimeout() { 
        return timeout;
    }
    
    public void setLocal(boolean local) { 
        this.local = local;
    }
    
    public boolean getLocal() { 
        return local;
    }
    
    public int incrementHops() { 
        return ++hops;
    }
    
}
