package ibis.cohort.impl.multithreaded;

import java.util.LinkedList;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Cohort;
import ibis.cohort.Event;

public class MTCohort implements Cohort {

    private LinkedList<ActivityRecord> available = new LinkedList<ActivityRecord>();
    
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

    public synchronized IDGenerator getIDGenerator() {
        
      //  System.out.println("New IDGenerator(" + startID +")");
        
        IDGenerator tmp = new IDGenerator(startID, startID+blockSize);
        startID += blockSize;
        return tmp;
    }

    public void send(ActivityIdentifier source, ActivityIdentifier target, Object o) {
        
        // TODO: improve this incredibly dumb implementation!
 
        workers[0].send(source, target, o);
    }
    
    void forwardEvent(Event e, int source) { 
 
        // TODO: improve this incredibly dumb implementation!
        
        int next = (source + 1) % workers.length;
        
        if (next == source) { 
            System.err.println("FAILED TO DELIVER EVENT! " + e);
            System.exit(1);
        } else { 
            workers[next].queueEvent(e);
        }
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
