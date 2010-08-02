package ibis.cohort;

import java.io.Serializable;

public abstract class Activity implements Serializable {

    private static final long serialVersionUID = -83331265534440970L;

    private static final byte REQUEST_UNKNOWN = 0;
    private static final byte REQUEST_SUSPEND = 1;
    private static final byte REQUEST_FINISH  = 2;
    
    protected transient Cohort cohort;
    
    private ActivityIdentifier identifier;
    private final Context context; 
    private final boolean restrictToLocal;
    
    private byte next = REQUEST_UNKNOWN;
        
    protected Activity(Context context, boolean restrictToLocal) { 
        this.context = context;
        this.restrictToLocal = restrictToLocal;
    }

    protected Activity(Context context) { 
        this(context, false);
    }
    
    public void initialize(ActivityIdentifier id) { 
        this.identifier = id;
    }
    
    public void setCohort(Cohort cohort) { 
        this.cohort = cohort;
    }
    
    public ActivityIdentifier identifier() {
        
        if (identifier == null) { 
            throw new IllegalStateException("Activity is not initialized yet"); 
        }
        
        return identifier;
    }

    public Cohort getCohort() {
        
        if (cohort == null) { 
            throw new IllegalStateException("Activity is not initialized yet"); 
        }
        
        return cohort;
    }
    
    public Context getContext() { 
        return context;
    }
  
    public boolean isRestrictedToLocal() { 
        return restrictToLocal;
    }
    
    public void reset() { 
        next = REQUEST_UNKNOWN;
    }
    
    public boolean mustSuspend() { 
        return (next == REQUEST_SUSPEND);
    }
    
    public boolean mustFinish() { 
        return (next == REQUEST_FINISH);
    }
       
    public void suspend() {
        
        if (next == REQUEST_FINISH) { 
            throw new IllegalStateException("Activity already requested to finish!");
        }
        
        next = REQUEST_SUSPEND;
    }    
    
    public void finish() {
        
        if (next == REQUEST_SUSPEND) { 
            throw new IllegalStateException("Activity already requested to suspend!");
        }
        
        next = REQUEST_FINISH;
    }    
    
    public abstract void initialize() throws Exception;
    
    public abstract void process(Event e) throws Exception;
   
    public abstract void cleanup() throws Exception;
    
    public abstract void cancel() throws Exception;
    
    public String toString() { 
        return identifier + " " + context;
    }
}
