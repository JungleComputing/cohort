package ibis.cohort.impl.distributed;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Cohort;
import ibis.cohort.CohortIdentifier;
import ibis.cohort.Context;
import ibis.cohort.Event;

public class MultiThreadedCohort implements Cohort {

    private final DistributedCohort parent;
    private final CohortIdentifier identifier;

    private CircularBuffer activitiesGoingDown = new CircularBuffer(16);
    private CircularBuffer activitiesGoingUp = new CircularBuffer(16);
    
    private CircularBuffer stealRequests = new CircularBuffer(16);

    private SingleThreadedCohort [] workers;
    private int nextSubmit = 0;

    public MultiThreadedCohort(DistributedCohort parent, 
            CohortIdentifier identifier, int workerCount) {

        this.parent = parent;
        this.identifier = identifier;

        if (workerCount == 0) { 

            String tmp = System.getProperty("ibis.cohort.workers");

            if (tmp != null && tmp.length() > 0) { 
                try { 
                    workerCount = Integer.parseInt(tmp);
                } catch (Exception e) {
                    System.err.println("Failed to parse property ibis.cohort.workers: " + e);
                }
            }

            if (workerCount == 0) { 
                // Automatically determine the number of cores to use
                workerCount = Runtime.getRuntime().availableProcessors();
            }
        }

        System.out.println("Starting MultiThreadedCohort using " + workerCount 
                + " workers");

        workers = new SingleThreadedCohort[workerCount];

        for (int i=0;i<workerCount;i++) { 
            workers[i] = new SingleThreadedCohort(this, 
                    parent.getCohortIdentifier());
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

        DistributedActivityIdentifier did = 
            (DistributedActivityIdentifier) e.target;

        CohortIdentifier target = did.getLastKnownCohort();

        for (int i=0;i<workers.length;i++) { 

            CohortIdentifier id = workers[i].identifier();

            if (target.equals(id)) { 
                workers[i].deliverEvent(e);
                return;
            }
        }

        System.err.println("EEP: failed to deliver event LOCATION: " 
                + identifier + " ERROR: " + e);

        //workers[((DistributedActivityIdentifier) e.target).getCohort().getWorkerID()].deliverEvent(e);
    }
    
    void addActivityRecord(ActivityRecord record, boolean local) { 
        
        if (record == null) { 
            System.out.println("EEP: steal reply is null!!");
            new Exception().printStackTrace();
        }

        if (local) { 
            synchronized (activitiesGoingUp) {
                activitiesGoingUp.insertLast(record);
            }
        } else { 
            synchronized (activitiesGoingDown) {
                activitiesGoingDown.insertLast(record);
            }
        }
    }

    // This one is top-down: a parent cohort is requesting work from below
    /*
    ActivityRecord stealRequest(CohortIdentifier source) {

        synchronized (activitiesGoingUp) {
            if (activitiesGoingUp.size() > 0) { 
                return (ActivityRecord) activitiesGoingUp.removeFirst();
            }
        }

        for (int i=0;i<workers.length;i++) { 
            workers[i].stealRequest(source);
        }

        return null;
    }
     */

    //  This one is top-down: a parent cohort is requesting work from below
    void postStealRequest(StealRequest request) {

        synchronized (stealRequests) {
            stealRequests.insertLast(request);
        }
        
        for (int i=0;i<workers.length;i++) { 
            workers[i].postStealRequest();
        }
    }
    
    // This one is bottom-up: a sub-cohort is requesting work from above 
    ActivityRecord stealAttempt(CohortIdentifier identifier) {

        // Check if we have any work queued at this level...
        synchronized (activitiesGoingDown) {
            if (activitiesGoingDown.size() > 0) { 
                return (ActivityRecord) activitiesGoingDown.removeFirst();
            }
        }

        synchronized (activitiesGoingUp) {
            if (activitiesGoingUp.size() > 0) { 
                return (ActivityRecord) activitiesGoingUp.removeFirst();
            }
        }

        // If not, we forward the steal request to our parent
        ActivityRecord tmp = parent.stealAttempt(null);

        if (tmp != null) { 
            return tmp;
        }

        for (int i=0;i<workers.length;i++) { 
            workers[i].stealRequest(identifier);
        }

        return null;
    }

    public CohortIdentifier identifier() {
        return identifier;
    }

    public boolean isMaster() {
        return parent.isMaster();
    }

    public Context getContext() {
        // TODO Auto-generated method stub
        return null;
    }

    public void setContext(Context context) {
        // TODO Auto-generated method stub

    }
}
