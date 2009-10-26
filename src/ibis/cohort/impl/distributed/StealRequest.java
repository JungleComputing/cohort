package ibis.cohort.impl.distributed;

import java.io.Serializable;

import ibis.cohort.CohortIdentifier;
import ibis.cohort.Context;

class StealRequest implements Serializable {
    
    private static final long serialVersionUID = 2655647847327367590L;
   
    public final CohortIdentifier remoteSource;
    public final Context context;
    public final int localSource;
      
    private transient long timeout = -1;
    private transient boolean stale = false;
    
    public StealRequest(final CohortIdentifier src, final Context context) {  
        // Use this for a remote steal request;
        super();
        this.remoteSource = src;
        this.context = context;
        this.localSource = -1;
    }
    
    public StealRequest(final int workerID, final Context context) {
        // Use this for a local steal request;
        super();
        this.localSource = workerID;
        this.context = context;
        this.remoteSource = null;
    }
    
    public synchronized void setTimeout(long timeout) { 
        this.timeout = timeout;
    }
    
    public synchronized long getTimeout() { 
        return timeout;
    }
  
    public synchronized boolean getStale() {
        return stale;
    }
    
    public synchronized boolean atomicSetStale() {
        boolean old = stale;
        stale = true;
        return old;
    }
    
    public boolean isLocal() { 
        return localSource != -1;
    }
}
