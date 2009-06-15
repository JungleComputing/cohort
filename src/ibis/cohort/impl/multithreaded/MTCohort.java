package ibis.cohort.impl.multithreaded;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Cohort;

public class MTCohort implements Cohort {
    
    private ComputationUnit [] workers;
    private int nextSubmit = 0;
    
    private long startID = 0;
    private long blockSize = 1000000;
    
    public MTCohort(int workerCount) { 
        workers = new ComputationUnit[workerCount];
       
        for (int i=0;i<workerCount;i++) { 
            workers[i] = new ComputationUnit(this, i);
        }
    
        for (int i=0;i<workerCount;i++) { 
            new Thread(workers[i]).start();
        }
    }
    
    public void cancel(ActivityIdentifier activity) {
        for (ComputationUnit u : workers) { 
            u.cancel(activity);
        }
    }

    public void cancelAll() {
        for (ComputationUnit u : workers) { 
            u.cancelAll();
        }
    }

    public void done() {
        for (ComputationUnit u : workers) { 
            u.done();
        }
    }

    public synchronized ActivityIdentifier submit(Activity a) {
 
        // We do a simple round-robin distribution of the jobs here.
        if (nextSubmit >= workers.length) { 
            nextSubmit = 0;
        }
        
        return workers[nextSubmit++].submit(a);
    }

    public synchronized IDGenerator getIDGenerator() {
        
      //  System.out.println("New IDGenerator(" + startID +")");
        
        IDGenerator tmp = new IDGenerator(startID, startID+blockSize);
        startID += blockSize;
        return tmp;
    }

    /*
    public void deliverEvent(Event e, int source) {
        // TODO: improve this incredibly dumb implementation!
        
        int target = (source+1) % workers.length;
        
        if (target == source) { 
            System.err.println("FAILED to deliver event!" + e);
        } else { 
            workers[target].queueEvent(e);
        }
    }

    public ActivityRecord stealAttempt(int source) {
        // TODO: improve this incredibly dumb implementation!
    
        int target = (source+1) % workers.length;
        
        if (target == source) { 
            System.err.println("FAILED steal request!");
            return null;
        } else { 
            return workers[target].steal();
        }
    }
*/
    public void send(ActivityIdentifier source, ActivityIdentifier target, Object o) {
        
        // TODO: improve this incredibly dumb implementation!
 
        workers[0].send(source, target, o);
    }
    
}
