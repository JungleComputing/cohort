package ibis.cohort.impl.distributed;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Cohort;
import ibis.cohort.CohortIdentifier;
import ibis.cohort.Event;

import java.util.LinkedList;

public class MultiThreadedCohort implements Cohort {

    private final DistributedCohort parent;
    private final CohortIdentifier identifier;
    
    private LinkedList<ActivityRecord> available = new LinkedList<ActivityRecord>();
    
    private SingleThreadedCohort [] workers;
    private int nextSubmit = 0;
   
    public MultiThreadedCohort(DistributedCohort parent, CohortIdentifier identifier, int workerCount) {
        
        this.parent = parent;
        this.identifier = identifier;
        
        if (workerCount == 0) { 
            // Automatically determine the number of cores to use
            workerCount = Runtime.getRuntime().availableProcessors();
        }
        
        System.out.println("Starting MultiThreadedCohort using " + workerCount + " workers");
        
        workers = new SingleThreadedCohort[workerCount];
       
        for (int i=0;i<workerCount;i++) { 
            workers[i] = new SingleThreadedCohort(this, parent.getCohortIdentifier());
        }
    
        for (int i=0;i<workerCount;i++) { 
            new Thread(workers[i], "Cohort ComputationUnit " + i).start();
        }
    }
    
    public void cancel(ActivityIdentifier activity) {
        for (SingleThreadedCohort u : workers) { 
            u.cancel(activity);
        }
    }

    public void done() {
        for (SingleThreadedCohort u : workers) { 
            u.done();
        }
    }

    public synchronized ActivityIdentifier submit(Activity a) {
 
       // System.out.println("MT submit");
        
        // We do a simple round-robin distribution of the jobs here.
        if (nextSubmit >= workers.length) { 
            nextSubmit = 0;
        }
        
        return workers[nextSubmit++].submit(a);
    }

    public DistributedActivityIdentifierGenerator getIDGenerator(CohortIdentifier identifier) {
        return parent.getIDGenerator(identifier);
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
        
        // TODO: optimize!
        
        CohortIdentifier target = ((DistributedActivityIdentifier) e.target).getCohort();
        
        for (int i=0;i<workers.length;i++) { 
            
            CohortIdentifier id = workers[i].identifier();
            
            if (target.equals(id)) { 
                workers[i].deliverEvent(e);
                return;
            }
        }
        
        System.err.println("EEP: failed to deliver event: " + e);
        
        //workers[((DistributedActivityIdentifier) e.target).getCohort().getWorkerID()].deliverEvent(e);
    }

    synchronized void addActivityRecord(ActivityRecord record) { 
        
        if (record == null) { 
            System.out.println("EEP: steal reply is null!!");
            new Exception().printStackTrace();
        }
        
        available.addLast(record);
    }   
    
    ActivityRecord stealAttempt(CohortIdentifier identifier) {
       
        int source = ((DistributedCohortIdentifier) identifier).getWorkerID();
        
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

    public CohortIdentifier identifier() {
        return identifier;
    }
    
    public boolean isMaster() {
        return parent.isMaster();
    }
}
