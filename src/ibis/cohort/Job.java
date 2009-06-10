package ibis.cohort;

import java.io.Serializable;

public abstract class Job implements Serializable {

    private static final long serialVersionUID = -83331265534440970L;

    protected transient Cohort cohort;
    private transient ResultHandler resultHandler;
    private transient Job parent;

    protected final JobIdentifier identifier;
    protected final Location location; 
    
    private boolean runnable = true;
    
    protected Job(Location location) { 
        identifier = JobIdentifier.getNext();
        this.location = location;
    }

    public JobIdentifier identifier() { 
        return identifier;
    }

    public Location getLocation() { 
        return location;
    }

    public synchronized void setResultHandler(ResultHandler r) {
        this.resultHandler = r;
    }

    public synchronized ResultHandler getResultHandler() {
        return resultHandler;
    }

    public synchronized void setCohort(Cohort japi) {
        this.cohort = japi;
    }

    protected synchronized Cohort getCohort() {
        return cohort;
    }

    protected synchronized void setParent(Job parent) { 
        this.parent = parent;
    }

    public synchronized Job getParent() { 
        return parent;
    }

    public synchronized boolean isRunnable() { 
        return runnable;
    }
    
    public synchronized void setRunnable(boolean value) { 
        runnable = value;
    }    
    
    public abstract Object produceResult();

    public abstract void run() throws Exception;
    
    public abstract void submitted();
    
    public abstract boolean isSubmitted();

    public abstract boolean isDone();

    
}
