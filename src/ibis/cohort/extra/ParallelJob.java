package ibis.cohort.extra;

import ibis.cohort.Context;
import ibis.cohort.Activity;
import ibis.cohort.Identifier;
import ibis.cohort.SubmissionException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public abstract class ParallelJob extends Activity {

    private static final long serialVersionUID = -5364517900077425503L;

    /* These are the possible states a paralleljob can reside in. 
       Valid state transitions are:

                                            +---+
                                            |   |
                                            v   |
       INITIAL -> SUBMITTED -> SPLITTING -> MERGE -> RESULT -> FINISHED
                                   |                   ^
                                   |                   | 
                                   +-------------------+
     */

    public enum State { 
        INITIAL, 
        SUBMITTED,
        SPLITTING,
        MERGING, 
        RESULT, 
        FINISHED; 

        public static boolean validTransition(State pre, State post) { 

            switch (pre) { 
            case INITIAL:
                return post == SUBMITTED;
            case SUBMITTED:
                return post == SPLITTING;                
            case SPLITTING:                
                return (post == MERGING || post == RESULT);
            case MERGING:
                return (post == MERGING || post == RESULT);
            case RESULT:
                return post == FINISHED;
            }

            return false;
        }
    }

    private class JobInfo { 
        final Activity job;
        Object result;
        
        JobInfo(Activity job) { 
            this.job = job;
        }
    }
    
    private HashMap<Identifier, JobInfo> subjobs; 
    private ArrayList<JobInfo> results; 
    
    private State state = State.INITIAL;

    private int totalSubJobs = 0;
    private int mergedResults = 0;

    protected ParallelJob(Context location) {
        super(location);
    }

    // We need a lock here, since there may be sever
    public synchronized void setState(State newState) { 

        if (!State.validTransition(state, newState)) { 
            throw new RuntimeException("Illegal state transition " + state 
                    + " -> " + newState);
        }

        state = newState;
    }

    public synchronized State getState() { 
        return state;
    }

    public synchronized int totalSubJobs() {
        return totalSubJobs;
    }

    /*
    // May only be called from split ? 
    public synchronized void submit(Activity job) throws SubmissionException {

        if (job.isSubmitted()) {
            throw new SubmissionException("Job submitted twice");
        }

        if (getState() != State.SPLITTING) { 
            throw new SubmissionException("Illegal job submission!"); 
        }

        job.setParent(this);

        if (subjobs == null) {
            subjobs = new HashMap<Identifier, JobInfo>();
        }

        totalSubJobs++;

        subjobs.put(job.identifier(), new JobInfo(job));
        
        cohort.submit(job, this);
    }

    public synchronized void storeResult(Activity job, Object result) {

        if (subjobs == null) { 
            throw new RuntimeException("Unexpected Job result");
        }

        JobInfo tmp = subjobs.remove(job.identifier());
        
        if (tmp == null) { 
            throw new RuntimeException("Unexpected Job result");
        }
        
        tmp.result = result;
        
        if (results == null) { 
            results = new ArrayList<JobInfo>();
        }
        
        results.add(tmp);
        
        cohort.unsuspend(this);
    }

    private synchronized JobInfo nextSubResult() {

        if (results == null || results.size() == 0) { 
            return null;
        }

        return results.remove(0);
    }
    
    public synchronized Object retrieveResult() {

        if (state != State.RESULT) { 
            throw new RuntimeException("Illegal result retrieval");
        }

        state = State.FINISHED;
        
        return produceResult();
    }
    
    public int cancelSubJob(Activity job) throws NoSuchChildException {
        return cancelSubJob(job.identifier());
    }
    
    public synchronized int cancelSubJob(Identifier job)
        throws NoSuchChildException {

        if (subjobs == null) {
            throw new NoSuchChildException("SubJob " + job
                    + " is not a child of " + identifier);
        }

        if (subjobs.remove(job) == null) { 
            throw new NoSuchChildException("SubJob " + job
                    + " is not a child of " + identifier);
        }

        totalSubJobs--;
        
        cohort.cancel(identifier);
                
        return subjobs.size();
    }

    public synchronized boolean cancelAllSubJobs() {

        totalSubJobs = 0;
        
        if (subjobs == null) {
            return false;
        }

        if (subjobs.size() > 0) {

            Set<Identifier> tmp = subjobs.keySet();
            
            for (Identifier id : tmp) {
                try { 
                    cancelSubJob(id);
                } catch (Exception e) {
                    // ignored
                }
            }
            
            subjobs.clear();
            return true;
        }

        return false;
    }

    public synchronized boolean isSubmitted() { 
        return (state != State.INITIAL);
    }

    public synchronized boolean isDone() { 
        return (state == State.FINISHED);
    }
        
    public synchronized void submitted() { 
        state = State.SUBMITTED;
    }
    
    public final void run() throws Exception {

        switch (state) {  
        case INITIAL:
            throw new RuntimeException("Running job that is not submitted!"); 

        case SUBMITTED:
            setState(State.SPLITTING);
            split();

            if (totalSubJobs > 0) { 
                setState(State.MERGING);
            } else { 
                setState(State.RESULT);
            }

            break;
            
        case MERGING:

            JobInfo tmp = nextSubResult(); 

            while (tmp != null) { 
                merge(tmp.job, tmp.result);
                mergedResults++;
                tmp = nextSubResult(); 
            }

            // NOTE: totalSubJobs may decrease when jobs are cancelled! 
            if (mergedResults >= totalSubJobs) { 
                setState(State.RESULT);
            }

            break;
            
        case RESULT:
            
            Object result = produceResult();
            
            setState(State.FINISHED);
            
            cohort.finished(this, result);
            
            break;
            
        case FINISHED: 
            throw new RuntimeException("Running job that is finished!");

        default:
            throw new RuntimeException("Running job with unknown state!");
        }
    }
   

    public abstract void split() throws Exception;
    public abstract void merge(Activity job, Object result);
*/
}
