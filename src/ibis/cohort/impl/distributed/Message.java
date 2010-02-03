package ibis.cohort.impl.distributed;

import ibis.cohort.ActivityIdentifier;
import ibis.cohort.CohortIdentifier;

import java.io.Serializable;

public abstract class Message implements Serializable {

    public final CohortIdentifier source;
    public CohortIdentifier target;
   
    private transient long timeout = -1;
    private transient boolean stale = false;
   
    protected Message(final CohortIdentifier source, 
            final CohortIdentifier target) {
        this.source = source;
        this.target = target;
    }
    
    protected Message(final CohortIdentifier source) {
        this.source = source;
    }
    
    public synchronized void setTarget(CohortIdentifier target) { 
        this.target = target;
    }
    
    public synchronized boolean isTargetSet() { 
        return (target != null);
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
  
    public boolean requiresLookup() {
        return false;
    }

    public boolean requiresRandomSelection() {
        return false;
    }

    public ActivityIdentifier targetActivity() {
        return null;
    }    
}
