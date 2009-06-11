package ibis.cohort;

import java.io.Serializable;

public abstract class Job implements Serializable {

    private static final long serialVersionUID = -83331265534440970L;

    protected transient Cohort cohort;
    private transient ResultHandler resultHandler;
    // private transient Job parent;

    private JobIdentifier identifier;
    
    private Context location; 
    
    private State state = State.INITIAL;
    
    public enum State { 
        INITIAL, 
        SUBMITTED, 
        RUNNABLE,
        SUSPENDED,
        FINISHED; 

        public static State checkTransition(State pre, State post) { 

            switch (pre) {
            case INITIAL:                
                if (post == SUBMITTED) {  
                    return post;
                }
                break;            
            case SUBMITTED:                
                if (post == RUNNABLE) {  
                    return post;
                }
                break;
            case RUNNABLE:                
                if (post == RUNNABLE || post == SUSPENDED || post == FINISHED) { 
                    return post;
                }
            case SUSPENDED:
                if (post == SUSPENDED || post == RUNNABLE) { 
                    return post;
                }
            }

            throw new IllegalStateException("Illegal state transition " + pre 
                    + " -> " + post);
        }
    }
    
    protected Job(Context location) { 
        this.location = location;
    }
    
    public JobIdentifier identifier() {
        
        if (state == State.INITIAL) { 
            throw new IllegalStateException("Job is not initialized yet"); 
        }
        
        return identifier;
    }

    public Context getLocation() { 
        return location;
    }

    public void setResultHandler(ResultHandler r) {
        this.resultHandler = r;
    }

    public ResultHandler getResultHandler() {
        return resultHandler;
    }

    public void setCohort(Cohort japi) {
        this.cohort = japi;
    }

    protected Cohort getCohort() {
        return cohort;
    }

    /*
    protected void setParent(Job parent) { 
        this.parent = parent;
    }

    public Job getParent() { 
        return parent;
    }
    */

    public boolean isSuspended() { 
        return state == State.SUSPENDED;
    }
    
    public boolean isRunnable() { 
        return state == State.RUNNABLE;
    }

    public boolean isFinished() { 
        return state == State.FINISHED;
    }
    
    public boolean isSubmitted() { 
        return state != State.INITIAL;
    }

    public void submitted() { 
        state = State.checkTransition(state, State.SUBMITTED);        
    }
    
    public void suspend() {
        state = State.checkTransition(state, State.SUSPENDED);
    }    
    
    public void runnable() {
        state = State.checkTransition(state, State.RUNNABLE); 
    }    

    public void finish() {
        state = State.checkTransition(state, State.FINISHED); 
    }    

    public void send(JobIdentifier target, Object o) { 
    	cohort.send(identifier, target, o);
    }
      
    public void deliver(Event e) { 
    	// TODO: store event
    	cohort.unsuspend(identifier);
    }
    
    public abstract void initialize() throws Exception;
    
    public abstract void process(Event e) throws Exception;
   
    public abstract void finalize() throws Exception;
    
    
}
