package ibis.cohort.impl.distributed;

import java.util.LinkedList;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Cohort;
import ibis.cohort.Event;
import ibis.ipl.IbisIdentifier;

public class MultiThreadedCohort implements Cohort {

    private final DistributedCohort parent;
    
    private LinkedList<ActivityRecord> available = new LinkedList<ActivityRecord>();
    
    private ComputationUnit [] workers;
    private int nextSubmit = 0;
   
    public MultiThreadedCohort(DistributedCohort parent, int workerCount) {
        
        this.parent = parent;
        
        if (workerCount == 0) { 
            // Automatically determine the number of cores to use
            workerCount = Runtime.getRuntime().availableProcessors();
        }
        
        workers = new ComputationUnit[workerCount];
       
        for (int i=0;i<workerCount;i++) { 
            workers[i] = new ComputationUnit(this, i);
        }
    
        for (int i=0;i<workerCount;i++) { 
            new Thread(workers[i], "Cohort ComputationUnit " + i).start();
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

    public IDGenerator getIDGenerator(int workerID) {
        return parent.getIDGenerator(workerID);
    }

    public void send(ActivityIdentifier source, ActivityIdentifier target, Object o) {

        /* 
        Identifier id = (Identifier) target;
        
        int workerID = id.getWorkerID();
        
        workers[workerID].send(source, target, o);
        */
        
        // SHOULD NOT BE CALLED IN THIS STACK!
        
        throw new IllegalStateException("Send called on MTCohort!");
    }
    
    void forwardEvent(Event e) {
        parent.forwardEvent(e);
    }
    
    void deliverEvent(Event e) { 
        workers[((Identifier) e.target).getWorkerID()].queueEvent(e);
    }

    // TODO: improve stealing ?
    synchronized void stealReply(ActivityRecord record) {
        
        if (record == null) { 
            System.out.println("EEP: steal reply is null!!");
            new Exception().printStackTrace();
        }
        
        available.addLast(record);
    }    

    ActivityRecord stealAttempt(int source) {
    
        synchronized (this) {
            if (available.size() > 0) { 
                return available.removeFirst();
            }
        }

        for (int i=0;i<workers.length;i++) { 
            if (i != source) { 
                workers[i].stealRequest();
            }
        }

        return null;
    }


}
