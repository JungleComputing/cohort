package ibis.cohort.impl.distributed.multi;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.ActivityIdentifierFactory;
import ibis.cohort.CancelEvent;
import ibis.cohort.Cohort;
import ibis.cohort.CohortIdentifier;
import ibis.cohort.Context;
import ibis.cohort.Event;
import ibis.cohort.MessageEvent;
import ibis.cohort.context.AndContext;
import ibis.cohort.context.ContextSet;
import ibis.cohort.context.UnitContext;
import ibis.cohort.extra.CircularBuffer;
import ibis.cohort.extra.CohortLogger;
import ibis.cohort.extra.CohortIdentifierFactory;
import ibis.cohort.extra.SimpleCohortIdentifierFactory;
import ibis.cohort.impl.distributed.ActivityRecord;
import ibis.cohort.impl.distributed.ApplicationMessage;
import ibis.cohort.impl.distributed.BottomCohort;
import ibis.cohort.impl.distributed.LocationCache;
import ibis.cohort.impl.distributed.LookupReply;
import ibis.cohort.impl.distributed.LookupRequest;
import ibis.cohort.impl.distributed.StealReply;
import ibis.cohort.impl.distributed.StealRequest;
import ibis.cohort.impl.distributed.TopCohort;
import ibis.cohort.impl.distributed.UndeliverableEvent;
import ibis.cohort.impl.distributed.single.SingleThreadedBottomCohort;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

public class MultiThreadedTopCohort implements Cohort, TopCohort {

//    private static final int REMOTE_STEAL_RANDOM = 0;
//    private static final int REMOTE_STEAL_ALL    = 1;

//    private static final int REMOTE_STEAL_POLICY = 
//        determineRemoteStealPolicy(REMOTE_STEAL_RANDOM);
    
    private BottomCohort [] workers;

    private final CohortIdentifier identifier;

    private final Random random = new Random();

    private final int workerCount;

    private boolean active = false;
    
    private Context [] contexts;
    private Context myContext;
    private boolean myContextChanged = false;
    
    private CircularBuffer myActivities = new CircularBuffer(16);

    private LocationCache locationCache = new LocationCache();
    
    private CohortIdentifierFactory cidFactory = 
        new SimpleCohortIdentifierFactory();

    private long startID = 0;
    private long blockSize = 1000000;
    
    private int nextSubmit = 0;
    
    private final LookupThread lookup;

    private CohortLogger logger;
    
    private class LookupThread extends Thread { 
        
        // NOTE: should use exp. backoff here!
        
        private static final int TIMEOUT = 100; // 1 sec. timeout. 

        private CircularBuffer newMessages = new CircularBuffer(1);    
        private CircularBuffer oldMessages = new CircularBuffer(1);
        
        LookupThread() { 
            super("LookupThread of " + identifier);
        }
        
        public synchronized void add(ApplicationMessage m) {
            newMessages.insertLast(m);
        }
        
        private boolean attemptDelivery(ApplicationMessage m) {
        
            ActivityIdentifier t = m.targetActivity();
            
            CohortIdentifier cid = locationCache.lookup(t);
            
            if (cid != null) { 
                
                BottomCohort b = getWorker(cid);
                
                if (b != null) { 
                    m.setTarget(cid);
                    b.deliverEventMessage(m);
                    return true;
                } else { 
                    warning("Cohort " + m.target + " not found! Redirecting application message!");
                    m.setTarget(null);
                }
            } 
     
            return false;
        }
        
        
        private int processOldMessages() { 
            
            final int size = oldMessages.size();
           
            for (int i=0;i<size;i++) { 
            
                ApplicationMessage m = 
                    (ApplicationMessage) oldMessages.removeFirst();
                
                if (!attemptDelivery(m)) { 
                    broadcastLookup(m.targetActivity());
                    oldMessages.insertLast(m);
                }
            }
            
            return size;
        }
        
        private synchronized int addNewMessages() { 
            
            final int size = newMessages.size();
            
            while (newMessages.size() > 0) { 
                ApplicationMessage m = 
                    (ApplicationMessage) newMessages.removeFirst();
        
                if (!attemptDelivery(m)) { 
                    oldMessages.insertLast(m);
                }
            }
            
            return size;
        }
        
        private synchronized void waitForTimeout() { 
            
            try { 
                wait(TIMEOUT);
            } catch (InterruptedException e) {
                // ignored
            }                
        }
        
        public void run() { 

            while (true) {
                waitForTimeout();
                int oldM = processOldMessages();                    
                int newM = addNewMessages();

                if ((oldM + newM) > 0) {
                    logger.debug("LOOKUP: " + oldM + " " + newM +
                                 " " + oldMessages.size());
                }
            }
        }
        
    }
    
    
/*    
    private static class StealState { 

        private StealRequest pending; 
        private long steals;
        private long succes;

        public synchronized boolean atomicSet(StealRequest s) {

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
*/
  
    /*
    private static int determineRemoteStealPolicy(int def) { 

        String tmp = System.getProperty("ibis.cohort.remotesteal.policy");

        if (tmp != null && tmp.length() > 0) {

            if (tmp.equals("all")) { 
                return REMOTE_STEAL_ALL;
            }

            if (tmp.equals("random")) { 
                return REMOTE_STEAL_RANDOM;
            }

            System.err.println("Failed to parse property " +
                    "ibis.cohort.remotesteal: " + tmp);
        }

        return def;
    }*/

    public MultiThreadedTopCohort(Properties p, int workerCount) {

        int count = workerCount;
        
        if (count == 0) {

            String tmp = p.getProperty("ibis.cohort.workers");

            if (tmp != null && tmp.length() > 0) {
                try {
                    count = Integer.parseInt(tmp);
                } catch (Exception e) {
                    logger.error("Failed to parse property " +
                                 "ibis.cohort.workers", e);
                }
            }

            if (count == 0) {
                // Automatically determine the number of cores to use
                count = Runtime.getRuntime().availableProcessors();
            }
        }

        this.identifier = cidFactory.generateCohortIdentifier();
        this.workerCount = count;

        this.logger = CohortLogger.getLogger(MultiThreadedTopCohort.class,
                                             identifier);
        logger.info("Starting MultiThreadedCohort using " + count + " workers");

        workers     = new SingleThreadedBottomCohort[count];
   //     localSteals = new StealState[count];
        contexts    = new Context[count];
      
        myContext = Context.ANY;
        myContextChanged = false;

        lookup = new LookupThread();
        lookup.start();
        
        
        
        for (int i = 0; i < count; i++) {
            workers[i] = new SingleThreadedBottomCohort(this, p, count, i, 
                    cidFactory.generateCohortIdentifier());
    //        localSteals[i] = new StealState();
            contexts[i] = Context.ANY;
        }
        
        logger.info("All workers created!");
    }

    private BottomCohort getWorker(CohortIdentifier cid) { 
        
        for (BottomCohort b : workers) { 
            if (cid.equals(b.identifier())) { 
                return b;
            }
        }

        return null;
    }
    
    /**
     * @deprecated Configure log4j to show line number
     */
    private void internalError(String msg) { 
        logger.fatal(msg, new Exception());
        System.exit(1);
    }

    /**
     * @deprecated Configure log4j to show line number
     */
    private void warning(String msg) { 
        logger.warn(msg, new Exception());
    }
        
    private void broadcastLookup(ActivityIdentifier id) { 
        
        LookupRequest r = new LookupRequest(identifier, id);
        
        for (int i=0;i<workerCount;i++) { 
            workers[i].deliverLookupRequest(r);
        }
    }

    
    private void enqueueMessage(ApplicationMessage m) { 
    
        // This is the place where all messages go that we cannot handle
        // directly.
        
        broadcastLookup(m.targetActivity());
        lookup.add(m);
    }
    
    private ActivityRecord getStoredActivity(Context c) {
        
        // TODO: improve performance ? 
        
        synchronized (myActivities) { 
            logger.debug("STEAL MT: activities " + myActivities.size());
            
            
            if (myActivities.size() > 0) { 
                if (c.isAny()) { 
                    return (ActivityRecord) myActivities.removeFirst();
                } else { 
                    for (int i=0;i<myActivities.size();i++) { 
                        ActivityRecord r = (ActivityRecord) myActivities.get(i); 
                        Context tmp = r.activity.getContext();
                        
                        if (c.contains(tmp)) { 
                            myActivities.remove(i);
                            return r;
                        }
                    }
                }
            }
            
        }
        
        return null;
    }
    
    public PrintStream getOutput() {
        return System.out;
    }

    private int selectTargetWorker() {
        // This return a random number between 0 .. workerCount-1
        return random.nextInt(workerCount);
    }
   
 

  /* 

    public ActivityIdentifierFactory getIDGenerator(
            CohortIdentifier identifier) {
   
    }
*/

    /*
    void undeliverableEvent(int workerID, Event e) {

        if (workerCount == 1) { 
            System.err.println("EEP: cannot forward undeliverable event! " + e);
            return;
        }

        int next = (workerID + 1) % workerCount;

        System.err.println("EEP: forwarding undeliverable event from " 
                + workerID + " to " + next);

        workers[next].deliverEvent(e);
    }
*/
  

    // This one is top-down: a parent cohort is requesting work from below
    /*
    void incomingRemoteStealRequest(StealRequest request) {

        switch (REMOTE_STEAL_POLICY) {

        case REMOTE_STEAL_ALL:
            for (int i=0;i<workers.length;i++) { 
                workers[i].postStealRequest(request);
            }
            return;

        case REMOTE_STEAL_RANDOM:
        default:
            // Notify a (random) worker that there is a steal request pending...
            workers[selectTargetWorker()].postStealRequest(request);
        return;
        } 
    }
*/
   
    /*
    
    void lookupReply(LookupRequest request, CohortIdentifier reply) {
        // eep
    }
    
    CohortIdentifier locateActivity(ActivityIdentifier id) { 
        
        synchronized (incomingRemoteActivities) {
            
            int size = incomingRemoteActivities.size();

            if (size == 0) {
                return null;
            }

            for (int i = 0; i < size; i++) {   

                ActivityIdentifier tmp = 
                    ((ActivityRecord) incomingRemoteActivities.get(i)).identifier();

                if (id.equals(tmp)) {
                    return identifier;
                }
            }
        }   
     
        return null;
        
        
    }
    
    */
    
    /*
    
    ActivityRecord getStoredRemoteActivity(Context threadContext) {

        synchronized (incomingRemoteActivities) {

            int size = incomingRemoteActivities.size();

            if (size == 0) {
                return null;
            }

            for (int i = 0; i < size; i++) {   

                ActivityRecord tmp = 
                    (ActivityRecord) incomingRemoteActivities.get(i);

                //        System.out.println("Returning stolen remote activity: " 
                //                + tmp.identifier().localName() + " contexts " + c + " " 
                //                + tmp.activity);

                if (threadContext.contains(tmp.activity.getContext())) {
                    incomingRemoteActivities.remove(i);
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

    */
    
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


   
   
/*


    public boolean idle(int workerID, Context threadContext) {

        // A worker has become idle and will remain so until we give it an 
        // event or activity. 

        // We first check the queue containing the activities stolen from remote
        // machines. We need to check the context to make sure that the idle 
        // worker can process it.
        ActivityRecord tmp = getStoredRemoteActivity(threadContext);

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
            StealRequest s = new StealRequest(workerID, threadContext);

            int target = selectTargetWorker();

            if (target == workerID) { 
                target = (target+1) % workerCount;
            }

            if (localSteals[workerID].atomicSet(s)) { 
                workers[target].postStealRequest(s);  
            }
        }

        // Next, we should also fire a remote steal. 
        parent.stealAttempt(threadContext);

        // Tell the worker I don't have work (yet). 
        return false;
    }
*/
    /*
    
    public boolean sendStealReply(StealRequest s, ActivityRecord a) {

        if (s.isLocal()) { 

            if (a != null) { 
                workers[s.localSource].deliverActivityRecord(a);
                localSteals[s.localSource].clear(true);
            } else { 
                localSteals[s.localSource].clear(false);
            }

            // We are always interested in the answer of local steals.
            return true;

        } else {

            // We check if the worker is still interested.  
            // If not, we keep the job to ourselves
            boolean stale = s.atomicSetStale();

            if (stale) {
                // The steal request was already answered by someone else
                return false;
            }

            // TODO: also check timeout here!

            parent.sendStealReply(new StealReply(s.remoteSource, a));

            return true;
        }
    }

    */
    
    
    
    
    /* ================= TopCohort interface =================================*/
    
    public synchronized void contextChanged(CohortIdentifier cid,
            Context newContext) {
        
        for (int i=0;i<workerCount;i++) { 
            
            if (cid.equals(workers[i].identifier())) {
                contexts[i] = newContext;
                myContextChanged = true;
                return;
            }
        }
        
        internalError("INTERNAL ERROR: Cohort " + cid + " not found!");
    }
    
    public synchronized ActivityIdentifierFactory 
        getActivityIdentifierFactory(CohortIdentifier cid) {

        ActivityIdentifierFactory tmp = new ActivityIdentifierFactory(
                cid.id,  startID, startID+blockSize);

        startID += blockSize;
        return tmp;
    }

    public LookupReply handleLookup(LookupRequest lr) {
        
        // Check if the activity location is cached         
        LocationCache.Entry e = locationCache.lookupEntry(lr.missing);

        if (e != null) { 
            return new LookupReply(identifier, lr.source, lr.missing, e.id, e.count);
        }
        
        for (int i=0;i<workerCount;i++) { 
            workers[i].deliverLookupRequest(lr);
        }
        
        return null;        
    }
        
    public void handleApplicationMessage(ApplicationMessage m) {
        logger.debug("ApplicationMessage for " + m.event.target + " at " + m.target);
        
        if (m.isTargetSet()) { 
            
            BottomCohort b = getWorker(m.target);
            
            if (b != null) { 
                logger.debug("DELIVER ApplicationMessage to " + b.identifier()); 
          
                b.deliverEventMessage(m);
                return;
            } else { 
                warning("Cohort " + m.target + " not found! Redirecting application message!");
                m.setTarget(null);
            }
        } 
       
        logger.debug("QUEUE ApplicationMessage for " + m.event.target); 
        enqueueMessage(m);
    }
    
    public void handleLookupReply(LookupReply m) { 
        // Cache locally!
        locationCache.put(m.missing, m.location, m.count);                   
        
        if (m.isTargetSet()) { 

            CohortIdentifier id = m.target;
            
            if (id.equals(identifier)) { 
                return;
            }
            
            BottomCohort b = getWorker(m.target);
            
            if (b != null) {
                b.deliverLookupReply(m);
                return;
            }
        } 
        
        warning("Dropping lookup reply! " + m);        
    }

    public void handleStealReply(StealReply m) { 

        if (m.isTargetSet()) { 
            
            BottomCohort b = getWorker(m.target);
            
            if (b != null) { 
                b.deliverStealReply(m);
                return;
            }
        }
         
        if (m.work != null) { 
            warning("Saving work before dropping StealReply: " + m.work.identifier());
                
            synchronized (myActivities) { 
                myActivities.insertLast(m.work);
            } 
        } else { 
            warning("Dropping empty StealReply");            
        }
    }

    public void handleUndeliverableEvent(UndeliverableEvent m) {
        // FIXME FIX!
        warning("ERROR: I just dropped an undeliverable event! " + m.event);
    }
    
    private int selectTargetWorker(CohortIdentifier exclude) { 
        
        if (workerCount == 1) { 
            return -1;
        }
        
        int rnd = selectTargetWorker();
        
        CohortIdentifier cid = workers[rnd].identifier();
        
        if (cid.equals(exclude)) { 
            return ((rnd+1)%workerCount);
        }
        
        return rnd;        
    }
   
    public ActivityRecord handleStealRequest(StealRequest sr) {

        logger.debug("STEAL_ARRIVED " + sr.context);
        
        ActivityRecord tmp = getStoredActivity(sr.context);
        
        if (tmp != null) { 
            logger.debug("STEAL_RETURN_LOCAL");
            return tmp;
        }

        int index = selectTargetWorker(sr.source);

        if (index >= 0) { 
           
            logger.debug("STEAL_FORWARD to " + workers[index].identifier());
            
            sr.setTarget(workers[index].identifier());
            workers[index].deliverStealRequest(sr);
        }
      
        return null;
    }
    
    public void handleWrongContext(ActivityRecord ar) {
        synchronized (myActivities) { 
            myActivities.insertLast(ar);
        } 
    }
    
    public CohortIdentifierFactory getCohortIdentifierFactory(
            CohortIdentifier cid) { 
        return cidFactory;
    }
    
    /* ================= End of TopCohort interface ==========================*/
    
    /* ================= Cohort interface ====================================*/

    public boolean isMaster() {
        return true;
    }
    
    public void done() {
        for (BottomCohort u : workers) {
            u.done();
        }
    }
    
    public CohortIdentifier identifier() {
        return identifier;
    }
    
    public synchronized ActivityIdentifier submit(Activity a) {
   
        // We do a simple round-robin distribution of the jobs here.
        if (nextSubmit >= workers.length) {
            nextSubmit = 0;
        }

        logger.debug("forward submit to " + workers[nextSubmit].identifier());
   
        return workers[nextSubmit++].deliverSubmit(a);
    }
    
    public void cancel(ActivityIdentifier activity) {
        send(new CancelEvent(activity));
    }
    
    public void send(ActivityIdentifier source, ActivityIdentifier target, Object o) {
        send(new MessageEvent(source, target, o));
    }
    
    public void send(Event e) {         

        ApplicationMessage m = new ApplicationMessage(identifier, e);
        
        // See if we can figure out who is running the activity         
        CohortIdentifier cid = locationCache.lookup(e.target);
        
        if (cid != null) {            
            m.setTarget(cid);
            
            BottomCohort b = getWorker(cid);
            
            if (b == null) { 
                logger.error(cid + " not found!");
                return;
            }
            
            b.deliverEventMessage(m);            
            return;
        } 
        
        // If we don't know the location of the activity we let a seperate 
        // thread handle it.  
        enqueueMessage(m);
    }
        
    public synchronized Context getContext() {
        
        if (!myContextChanged) { 
            return myContext;
        } else { 

            ContextSet tmp = null;

            for (int i=0;i<workerCount;i++) {

                Context other = workers[i].getContext();

                if (other.isUnit()) {

                    if (tmp == null) { 
                        tmp = new ContextSet((UnitContext) other);
                    } else { 
                        tmp.add((UnitContext) other);
                    }
                } else if (other.isAnd()) { 

                    if (tmp == null) { 
                        tmp = new ContextSet((AndContext) other);
                    } else { 
                        tmp.add((AndContext) other);
                    }
                } else if (other.isSet()) {

                    if (tmp == null) { 
                        tmp = new ContextSet((ContextSet) other);
                    } else { 
                        tmp.add((ContextSet) other);
                    }
                }
            }

            myContextChanged = false;
            
            if (tmp == null) { 
                return Context.ANY;
            } else { 
                return tmp;
            }
        } 
    }
    
    public Cohort[] getSubCohorts() {
        return (Cohort []) workers.clone();
    }
    
    public boolean activate() { 

        synchronized (this) {
            if (active) { 
                return false;
            }

            active = true;
        }

        for (int i = 0; i < workerCount; i++) {
            logger.info("Activating worker " + i);
            workers[i].activate();
        }

        return true;
    }
    
    
    public boolean deregister(String name, Context scope) {
        // TODO Auto-generated method stub
        return false;
    }

    public ActivityIdentifier lookup(String name, Context scope) {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean register(String name, ActivityIdentifier id, Context scope) {
        // TODO Auto-generated method stub
        return false;
    }

    public void setContext(Context context) throws Exception {
        throw new Exception("Cannot set context of this Cohort!");
    }

    public synchronized void setContext(CohortIdentifier id, Context context) 
        throws Exception {
        
        BottomCohort b = getWorker(id);
        
        if (b == null) {
            throw new Exception("Cohort " + id + " not found!");
        } 
        
        b.setContext(id, context);
    }

        public CohortIdentifier [] getLeafIDs() { 
        
        ArrayList<CohortIdentifier> tmp = new ArrayList<CohortIdentifier>();
        
        for (BottomCohort w : workers) { 
            CohortIdentifier [] ids = w.getLeafIDs();
            
            for (CohortIdentifier id : ids) { 
                tmp.add(id);
            }
        }
    
        return tmp.toArray(new CohortIdentifier[tmp.size()]);
    }
   

    /* ================= End of Cohort interface =============================*/
}
