package ibis.cohort.impl.distributed;

import java.io.PrintStream;
import java.util.Random;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Cohort;
import ibis.cohort.CohortIdentifier;
import ibis.cohort.Context;
import ibis.cohort.Event;

public class MultiThreadedCohort implements Cohort {

    private final DistributedCohort parent;

    private final CohortIdentifier identifier;

    private final Random random = new Random();

    private final int workerCount;

    private CircularBuffer incomingActivities = new CircularBuffer(16);

    private SingleThreadedCohort[] workers;

    private static class StealState { 
        
        private LocalStealRequest pending; 
        private long steals;
        private long succes;
        
        public synchronized boolean atomicSet(LocalStealRequest s) {
            
            if (pending != null) {
                return false;
            }
           
            pending = s;
            steals++;
            return true;
        }
        
        public synchronized void clear(boolean succes) {
            
            pending = null;
            
            if (succes) { 
                this.succes++;
            }
        }
        
        public synchronized boolean pending() {
            return (pending != null);
        }
    }
    
    private StealState [] localSteals;
    
   // private long printLoadDeadLine = -1;
    
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
                    System.err.println("Failed to parse property " +
                                "ibis.cohort.workers: " + e);
                }
            }

            if (workerCount == 0) {
                // Automatically determine the number of cores to use
                workerCount = Runtime.getRuntime().availableProcessors();
            }
        }

        this.workerCount = workerCount;
        
        System.out.println("Starting MultiThreadedCohort using " + workerCount
                + " workers");

        workers = new SingleThreadedCohort[workerCount];
        localSteals = new StealState[workerCount];
        
        for (int i = 0; i < workerCount; i++) {
            workers[i] = new SingleThreadedCohort(this, i, parent
                    .getCohortIdentifier());
        
            localSteals[i] = new StealState();
        }

        for (int i = 0; i < workerCount; i++) {
            new Thread(workers[i], "Cohort ComputationUnit " + i).start();
        }
    }

    public PrintStream getOutput() {
        return System.out;
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

    public DistributedActivityIdentifierGenerator getIDGenerator(
            CohortIdentifier identifier) {
        return parent.getIDGenerator(identifier);
    }

    public void send(ActivityIdentifier source, ActivityIdentifier target,
            Object o) {

        /*
         * Identifier id = (Identifier) target;
         * 
         * int workerID = id.getWorkerID();
         * 
         * workers[workerID].send(source, target, o);
         */

        // SHOULD NOT BE CALLED IN THIS STACK!
        throw new IllegalStateException("Send called on MTCohort!");
    }

    void forwardEvent(Event e) {
        parent.forwardEvent(e);
    }
    
    void undeliverableEvent(int workerID, Event e) {
        
        int next = (workerID + 1) % workerCount;
        
        System.err.println("EEP: forwarding undeliverable event from " 
                + workerID + " to " + next);
     
        workers[next].deliverEvent(e);
    }

    void deliverEvent(Event e) {

        // TODO: optimize!

        DistributedActivityIdentifier did = (DistributedActivityIdentifier) e.target;

        CohortIdentifier target = did.getLastKnownCohort();

        for (int i = 0; i < workers.length; i++) {

            CohortIdentifier id = workers[i].identifier();

            if (target.equals(id)) {
                workers[i].deliverEvent(e);
                return;
            }
        }

        System.err.println("EEP: failed to deliver event LOCATION: "
                + identifier + " ERROR: " + e);

        // workers[((DistributedActivityIdentifier)
        // e.target).getCohort().getWorkerID()].deliverEvent(e);
    }

    void addActivityRecord(CohortIdentifier target, ActivityRecord record) {

        if (record == null) {
            System.out.println("EEP: steal reply is null!!");
            new Exception().printStackTrace();
        }

        // NOTE: for the moment we store all incoming jobs here. Any idle
        // workers will poll us for work anyway.
        synchronized (incomingActivities) {
            incomingActivities.insertLast(record);
        }
    }

    // This one is top-down: a parent cohort is requesting work from below
    /*    idleTime = new long[workerCount];


     * ActivityRecord stealRequest(CohortIdentifier source) {
     * 
     * synchronized (activitiesGoingUp) { if (activitiesGoingUp.size() > 0) {
     * return (ActivityRecord) activitiesGoingUp.removeFirst(); } }
     * 
     * for (int i=0;i<workers.length;i++) { workers[i].stealRequest(source); }
     * 
     * return null; }
     */

    private int selectTargetWorker() {
        // This return a random number between 0 .. workerCount-1
        return random.nextInt(workerCount);
    }

    // This one is top-down: a parent cohort is requesting work from below
    void postStealRequest(StealRequest request) {

        // Notify a (random) worker that there is a steal request pending...
       // workers[selectTargetWorker()].postStealRequest(request);
  
        // FIXME FIXME implement!
    }

    /*
    // Send result of steal back to requester.
    void sendStealReply(StealRequest r, ActivityRecord a) {
        parent.sendStealReply(new StealReply(r.src, a));
    }

    // Send result of steal back to requester.
    void sendStealReply(StealRequest r) {
        parent.sendStealReply(new StealReply(r.src, null));
    }
    */

    /*
    // Forward unsuccesful steal request to next worker.
    public void returnStealRequest(int workerID, StealRequest request) {

        if (request.incrementHops() == workerCount) {
            // All workers have seen this request, no work was found!
            sendStealReply(request);
            return;
        }

        // forward steal request to next worker.
        workers[(workerID + 1) % workerCount].postStealRequest(request);
    }
*/
    
    ActivityRecord getStoredActivity(Context c) {

        synchronized (incomingActivities) {

            int size = incomingActivities.size();

            if (size == 0) {
                return null;
            }

            for (int i = 0; i < size; i++) {   
                
                ActivityRecord tmp = (ActivityRecord) incomingActivities.get(i);

                System.out.println("Activity: " + tmp + " contexts " + c + " " + tmp.activity);
                
                if (c.match(tmp.activity.getContext())) {
                    incomingActivities.remove(i);
                    return tmp;
                }
            }

            // No matching activities found....

            // FIXME: This approach may lead to starvation if an activity gets
            // stuck on a machine that has no (remaining) workers that can
            // process it....
            return null;
        }
    }

    /*
    // This one is bottom-up: a sub-cohort is requesting work from above
    ActivityRecord stealAttempt(int workerID, StealRequest sr) {

        // We first check the local queue for work. Since we don't know where
        // it's coming from, we need to check the context to make sure that we
        // can process it.
        ActivityRecord tmp = getStoredActivity(sr.context);

        if (tmp != null) { 
            return tmp;
        }
        
        // If not, we forward the steal request to our parent
        parent.stealAttempt(sr);

        // Next, we ask the one of the other local workers. > 0
        if (workerCount > 1) {

            int index = selectTargetWorker();

            if (index == workerID) {
                index = (index + 1) % workerCount;
            }

            System.err.println("Local steal posted at worker " + index);
            
            workers[index].postStealRequest(sr);
        }

        return null;
    }
*/
    
    
    public CohortIdentifier identifier() {
        return identifier;
    }

    public boolean isMaster() {
        return parent.isMaster();
    }

    public Context getContext() {
        throw new IllegalStateException("getContext not allowed!");
    }

    public void setContext(Context context) {
        throw new IllegalStateException("setContext not allowed!");
    }
    
    public boolean idle(int workerID, Context c) {
      
        // A worker has become idle and will remain so until we give it an 
        // event or activity. 
        
        // We first check the local queue for work. We need to check the 
        // context to make sure that the idle worker can process it.
        ActivityRecord tmp = getStoredActivity(c);

        if (tmp != null) {
            workers[workerID].deliverActivityRecord(tmp);
            // Tell the worker it should have work now 
            return true;
        }
        
        // If we don't have any work waiting, so we need to steal some.
        
        // If we have more than 1 worker we can steal locally
        if (workerCount > 1) { 
        
            // First make sure that there are no (local) steal request pending.
            if (localSteals[workerID].pending()) { 
                // Tell the worker I don't have work (yet)
                return false;
            }

            // No pending request, so notify all other local workers that we need 
            // work. Register each request so we can (async) wait for the reply. 
            LocalStealRequest s = new LocalStealRequest(workerID, c);

            int target = selectTargetWorker();

            if (target == workerID) { 
                target = (target+1) % workerCount;
            }

            if (localSteals[workerID].atomicSet(s)) { 
                workers[target].postStealRequest(s);  
            }
        }
        
        // Next, we should also fire a remote steal. FIXME FIXME
        // parent.stealAttempt(c);
        
        // Tell the worker I don't have work (yet). 
        return false;
    }

    public void stealReply(int workerID, LocalStealRequest s, ActivityRecord a) {
    
        // FIXME: Maybe we should check if the worker is still interested ? 
        // If not, we can keep the job to ourselves!
        
        if (a != null) { 
            workers[s.workerID].deliverActivityRecord(a);
            localSteals[s.workerID].clear(true);
        } else { 
            localSteals[s.workerID].clear(false);
        }
    }
    
    /*
    synchronized void printLoad() { 
        
        long time = System.currentTimeMillis();
        
        if (time > printLoadDeadLine) { 
            System.out.println("Load at " + time + " : " + (workerCount-idle) 
                    + " " + sleepTime + " " + sleepCount + " " 
                    + sleepTime/sleepCount);
            printLoadDeadLine = time + 500;
     
            sleepTime = 0;
            sleepCount = 0;
        }
    }
    
    /*
    synchronized void workerIdle(int workerID, long sleepTime) {
        idle++;
        
        this.sleepTime += sleepTime;
        sleepCount++;
        
        long time = System.currentTimeMillis();
        
        if (time > printLoadDeadLine) { 
            System.out.println("Load at " + time + " : " + (workerCount-idle) 
                    + " " + sleepTime + " " + sleepCount + " " 
                    + sleepTime/sleepCount);
            printLoadDeadLine = time + 500;
     
            sleepTime = 0;
            sleepCount = 0;
        }
    }

    synchronized void workerActive(int workerID) {
        idle--;
    
        long time = System.currentTimeMillis();
 
        if (time > printLoadDeadLine) { 
     
            
            System.out.println("Load at " + time + " : " + (workerCount-idle) 
                    + " " + sleepTime + " " + sleepCount + " " 
                    + (sleepCount > 0 ? sleepTime/sleepCount : 0));
            
            printLoadDeadLine = time + 500;
     
            sleepTime = 0;
            sleepCount = 0;
     
        }
    }*/
    
    
}
