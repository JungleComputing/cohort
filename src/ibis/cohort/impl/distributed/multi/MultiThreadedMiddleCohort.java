package ibis.cohort.impl.distributed.multi;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.ActivityIdentifierFactory;
import ibis.cohort.CohortIdentifier;
import ibis.cohort.Context;
import ibis.cohort.context.AndContext;
import ibis.cohort.context.OrContext;
import ibis.cohort.context.UnitContext;
import ibis.cohort.extra.CircularBuffer;
import ibis.cohort.extra.CohortIdentifierFactory;
import ibis.cohort.extra.CohortLogger;
import ibis.cohort.extra.Debug;
import ibis.cohort.extra.SmartWorkQueue;
import ibis.cohort.extra.SynchronizedWorkQueue;
import ibis.cohort.extra.WorkQueue;
import ibis.cohort.extra.WorkQueueFactory;
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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

//FIXME: crap name!
public class MultiThreadedMiddleCohort implements TopCohort, BottomCohort {

//  private static final int REMOTE_STEAL_RANDOM = 0;
//  private static final int REMOTE_STEAL_ALL    = 1;

//  private static final int REMOTE_STEAL_POLICY = 
//  determineRemoteStealPolicy(REMOTE_STEAL_RANDOM);

    private static boolean PUSHDOWN_SUBMITS = false;
    
    private final TopCohort parent;

    private ArrayList<BottomCohort> incomingWorkers;

    private BottomCohort [] workers;
    private int workerCount;
    
    private final CohortIdentifier identifier;

    private final Random random = new Random();

    private boolean active = false;

    private Context [] contexts;
    private Context myContext;
    private boolean myContextChanged = false;

   // private ActivityRecordQueue myActivities = new ActivityRecordQueue();

    private final WorkQueue myActivities;
    
    private LocationCache locationCache = new LocationCache();

    private final CohortIdentifierFactory cidFactory;

    private ActivityIdentifierFactory aidFactory;
    
    private int nextSubmit = 0;

    private final LookupThread lookup;

    private final CohortLogger logger;

    private class LookupThread extends Thread { 

        // NOTE: should use exp. backoff here!

        private static final int TIMEOUT = 1000; // 1 sec. timeout. 

        private boolean done = false;

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

                m.setTarget(cid);

                BottomCohort b = getWorker(cid);

                if (b != null) { 
                    b.deliverEventMessage(m);
                    return true;
                } else { 
                    parent.handleApplicationMessage(m);
                    return true;
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
                    
                    System.out.println("Trying to deliver to " + m.targetActivity());
                    
                    triggerLookup(m.targetActivity());
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

        private synchronized boolean waitForTimeout() { 

            try { 
                wait(TIMEOUT);
            } catch (InterruptedException e) {
                // ignored
            }                
            
            return done;
        }

        public synchronized void done() { 
            done = true;
        }

        public void run() { 

            boolean done = false;
            
            while (!done) {
                int oldM = processOldMessages();                    
                int newM = addNewMessages();

                if ((oldM + newM) > 0) {
                    if (Debug.DEBUG_LOOKUP) { 
                        logger.info("LOOKUP: " + oldM + " " + newM + " " 
                                + oldMessages.size());
                    }
                }
            
                done = waitForTimeout();
            }

            if (Debug.DEBUG_LOOKUP) { 
                logger.info("LOOKUP terminating.");
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

    public MultiThreadedMiddleCohort(TopCohort parent, Properties p) throws Exception {

        int count = 0;
        
        this.parent = parent;

        cidFactory = parent.getCohortIdentifierFactory(null);
        identifier = cidFactory.generateCohortIdentifier();
        aidFactory = getActivityIdentifierFactory(identifier);        
        
        String tmp = p.getProperty("ibis.cohort.submit.pushdown");

        if (tmp != null) {

            try { 
                PUSHDOWN_SUBMITS = Boolean.parseBoolean(tmp);
            } catch (Exception e) {
                System.err.println("Failed to parse " +
                        "ibis.cohort.submits.pushdown: " + tmp);
            }
        }
        
        tmp = p.getProperty("ibis.cohort.workqueue");

        myActivities = WorkQueueFactory.createQueue(tmp, true, "M(" + identifier + ")");
        
        this.logger = CohortLogger.getLogger(MultiThreadedMiddleCohort.class, identifier);
        
         incomingWorkers = new ArrayList<BottomCohort>();
        //     localSteals = new StealState[count];
        contexts    = new Context[count];

        myContext = UnitContext.DEFAULT;
        myContextChanged = false;

        lookup = new LookupThread();
        lookup.start();

        /*
        for (int i = 0; i < count; i++) {
            workers[i] = new SingleThreadedBottomCohort(this, p, 
                    cidFactory.generateCohortIdentifier());
            //        localSteals[i] = new StealState();
            contexts[i] = Context.ANY;
        }
         */

        logger.warn("Starting MultiThreadedMiddleCohort " + identifier + " / " + myContext);

        parent.register(this);
    }

    private BottomCohort getWorker(CohortIdentifier cid) { 

        for (BottomCohort b : workers) { 
            if (cid.equals(b.identifier())) { 
                return b;
            }
        }

        return null;
    }


    /*
    private CohortIdentifier performLookup(ActivityIdentifier id) { 

        if (DEBUG) { 
            logger.info("LOOKUP searching " + id);
        }

        LookupRequest r = new LookupRequest(identifier, id);

        if (DEBUG) { 
            logger.info("FORWARD LOOKUP to parent searching " + id);
        }

        CohortIdentifier cid = parent.handleLookup(r);

        if (cid != null) { 
            return cid;
        }

        if (DEBUG) { 
            logger.info("LOCAL BROADCAST LOOKUP searching " + id);
        }

        for (int i=0;i<workerCount;i++) { 
            workers[i].deliverLookupRequest(r);
        }

        return null;
    }
     */

    private void triggerLookup(ActivityIdentifier id) {

        System.out.println("Sending lookuprequest for " + id);
        
        // Send a lookup request to parent and all children, regardless of 
        // whether anything is cached... 
        LookupRequest lr = new LookupRequest(identifier, id);

        // Check if the parent has cached the location         
        LookupReply tmp = parent.handleLookup(lr);

        if (tmp != null) {
            locationCache.put(tmp.missing, tmp.location, tmp.count);
            return;
        }

        // Check if any of the childeren owns the activity         
        for (int i=0;i<workerCount;i++) { 
            workers[i].deliverLookupRequest(lr);
        }
    }

    private void enqueueMessage(ApplicationMessage m) { 
        
        System.out.println("Queued message for" + m.targetActivity() + " from " + m.source);
        
        triggerLookup(m.targetActivity());
        lookup.add(m);
    }

    /*
    private ActivityRecord getStoredActivity(Context c) {

        // TODO: improve performance ? 

        synchronized (myActivities) { 

            System.out.println("STEAL MT: activities " + myActivities.size());


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
    }*/

    public PrintStream getOutput() {
        return System.out;
    }

    private int selectTargetWorker() {
        // This return a random number between 0 .. workerCount-1
        return random.nextInt(workerCount);
    }

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

        logger.warning("Cohort " + cid + " not found!");
    }

    public synchronized ActivityIdentifierFactory 
            getActivityIdentifierFactory(CohortIdentifier cid) {
        return parent.getActivityIdentifierFactory(cid);
    }

    public LookupReply handleLookup(LookupRequest lr) {

        // Check if the activity location is cached locally     
        LocationCache.Entry e = locationCache.lookupEntry(lr.missing);

        if (e != null) { 
            return new LookupReply(identifier, lr.source, lr.missing, e.id, e.count);
        }

        // Check if the parent has cached the location         
        LookupReply tmp = parent.handleLookup(lr);

        if (tmp != null) {
            locationCache.put(tmp.missing, tmp.location, tmp.count);
            return tmp;
        }

        // Check if any of the childeren owns the activity         
        for (int i=0;i<workerCount;i++) { 
            workers[i].deliverLookupRequest(lr);
        }

        return null;        
    }

    public void handleApplicationMessage(ApplicationMessage m) {

        if (Debug.DEBUG_EVENTS) { 
            logger.info("EventMessage for " + m.event.target + " at " + m.target);
        }

        if (m.isTargetSet()) { 

            BottomCohort b = getWorker(m.target);

            if (b != null) { 

                if (Debug.DEBUG_EVENTS) { 
                    logger.info("DELIVERED " + "EventMessage to " + b.identifier()); 
                }
                b.deliverEventMessage(m);
                return;
            }

            parent.handleApplicationMessage(m);
            return;
        } 

        if (Debug.DEBUG_EVENTS) { 
            logger.info("QUEUE " + "EventMessage for " + m.event.target); 
        }

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

            parent.handleLookupReply(m);
            return;
        } 

        logger.warning("Dropping lookup reply! " + m);        
    }

    public void handleStealReply(StealReply m) { 

        logger.warn("MT handling STEAL REPLY " + m.isTargetSet() + " " + m.isEmpty());
       
        if (m.isTargetSet()) { 

            BottomCohort b = getWorker(m.target);

            if (b != null) { 
                
                logger.warn("MT handling STEAL REPLY target is local! " + m.target);
                
                b.deliverStealReply(m);
                return;
            }

            logger.warn("MT handling STEAL REPLY target is remote! " + m.target);
            
            parent.handleStealReply(m);
        } else {
            // Should never happen ?
            if (!m.isEmpty()) { 
                logger.warning("Saving work before dropping StealReply");
                myActivities.enqueue(m.getWork());
            } else { 
                logger.warning("Dropping empty StealReply");            
            }
        }
    }

    public void handleUndeliverableEvent(UndeliverableEvent m) {
        // FIXME FIX!
        logger.fixme("I just dropped an undeliverable event! " + m.event);
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

      //  System.out.println("MT STEAL");
        
        if (Debug.DEBUG_STEAL) { 
            logger.info("M STEAL REQUEST from child " + sr.source + " with context " 
                    + sr.context);
        }

        ActivityRecord tmp = myActivities.steal(sr.context);

        if (tmp != null) { 

            if (Debug.DEBUG_STEAL) { 
                logger.info("M STEAL REPLY (LOCAL) " + tmp.identifier() 
                        + " for child " + sr.source);
            }
            return tmp;
        }

        tmp = parent.handleStealRequest(sr);

        if (tmp != null) {
            if (Debug.DEBUG_STEAL) { 
                logger.info("M STEAL REPLY (PARENT) " + tmp.identifier() 
                        + " for child " + sr.source);
            }
            return tmp;
        }   

        int index = selectTargetWorker(sr.source);

        if (index >= 0) { 

            StealRequest copy = new StealRequest(sr.source, sr.context);

            if (Debug.DEBUG_STEAL) { 
                logger.info("M FORWARD STEAL from child " + sr.source 
                        + " to child " + workers[index].identifier());            
            }

            copy.setTarget(workers[index].identifier());
            workers[index].deliverStealRequest(copy);
        }

        if (Debug.DEBUG_STEAL) { 
            logger.info("M STEAL REPLY (EMPTY) for child " + sr.source);
        }   

        return null;
    }

    public void handleWrongContext(ActivityRecord ar) {
        myActivities.enqueue(ar);
    }

    public CohortIdentifierFactory getCohortIdentifierFactory(
            CohortIdentifier cid) {
        return parent.getCohortIdentifierFactory(cid);        
    }

    public synchronized void register(BottomCohort cohort) throws Exception {

        if (active) { 
            throw new Exception("Cannot register new BottomCohort while " +
                        "TopCohort is active!");
        }
        
        incomingWorkers.add(cohort);
    }

    
    /* ================= End of TopCohort interface ==========================*/





    /* ================= BottomCohort interface ==============================*/

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

    public synchronized void setContext(CohortIdentifier id, Context context) 
    throws Exception {

        if (Debug.DEBUG_CONTEXT) { 
            logger.info("Setting context of " + id + " to " + context);
        }

        BottomCohort b = getWorker(id);

        if (b == null) {
            throw new Exception("Cohort " + id + " not found!");
        } 

        b.setContext(id, context);
    }

    public synchronized Context getContext() {

        if (!myContextChanged) { 
            return myContext;
        } else { 
                
            Context [] tmp = new Context[workerCount];
                
            for (int i=0;i<workerCount;i++) {
                tmp[i] = workers[i].getContext();
            }

            myContext = OrContext.merge(tmp);
            myContextChanged = false;
            return myContext;
        } 
    }
    
    public CohortIdentifier identifier() {
        return identifier;
    }

    public boolean activate() { 

        synchronized (this) {
            if (active) { 
                return false;
            }

            active = true;
            workerCount = incomingWorkers.size();
            workers = incomingWorkers.toArray(new BottomCohort[workerCount]);
            
            // No workers may be added after this point
            incomingWorkers = null;
        }

        for (int i = 0; i < workerCount; i++) {
            logger.info("Activating worker " + i);
            workers[i].activate();
        }

        return true;
    }

    public void done() {

        logger.warn("done");
        
        lookup.done();
        
        if (active) { 
            for (BottomCohort u : workers) {
                u.done();
            }
        } else { 
            for (BottomCohort u : incomingWorkers) {
                u.done();
            }
        }
    }

    public boolean canProcessActivities() {
        return false;
    }

    private synchronized ActivityIdentifier createActivityID() {

        try {
            return aidFactory.createActivityID();
        } catch (Exception e) {
            // Oops, we ran out of IDs. Get some more from our parent!
            aidFactory = getActivityIdentifierFactory(identifier);
        }

        try {
            return aidFactory.createActivityID();
        } catch (Exception e) {
            throw new RuntimeException(
                    "INTERNAL ERROR: failed to create new ID block!", e);
        }
    }

    public synchronized ActivityIdentifier deliverSubmit(Activity a) {

        if (PUSHDOWN_SUBMITS)  { 

            if (Debug.DEBUG_SUBMIT) { 
                logger.info("M PUSHDOWN SUBMIT activity with context " + a.getContext());
            }

            // We do a simple round-robin distribution of the jobs here.
            if (nextSubmit >= workers.length) {
                nextSubmit = 0;
            }

            if (Debug.DEBUG_SUBMIT) { 
                logger.info("FORWARD SUBMIT to child " 
                        + workers[nextSubmit].identifier());
            }

            return workers[nextSubmit++].deliverSubmit(a);
        }
        
        if (Debug.DEBUG_SUBMIT) { 
            logger.info("M LOCAL SUBMIT activity with context " + a.getContext());
        }

        ActivityIdentifier id = createActivityID();
        a.initialize(id);

        if (Debug.DEBUG_SUBMIT) {
            logger.info("created " + id + " at " 
                    + System.currentTimeMillis() + " from DIST"); 
        }
     
        return id;
    }

    public void deliverStealRequest(StealRequest sr) {

        // FIXME: hardcodes steal size!
    
        if (Debug.DEBUG_STEAL) { 
            logger.info("M REMOTE STEAL REQUEST from child " + sr.source 
                    + " context " + sr.context);
        }

        ActivityRecord [] tmp = myActivities.steal(sr.context, 10);
    
        if (tmp != null && tmp.length > 0) { 

            if (Debug.DEBUG_STEAL) { 
                logger.info("M SUCCESFULL REMOTE STEAL REQUEST from child " + sr.source 
                        + " context " + sr.context);
            }
            parent.handleStealReply(new StealReply(identifier, sr.source, tmp));
            return;
        }

        CohortIdentifier cid = sr.target;
        BottomCohort b = null;

        if (cid != null) { 
            
            if (Debug.DEBUG_STEAL) { 
                logger.info("M RECEIVED REMOTE STEAL REQUEST with TARGET " + 
                        cid + " context " + sr.context);
            }
            
            b = getWorker(cid);
        } 

        // If we do not have a specific worker to steal from we'll just pick 
        // one at random...

        // FIXME FIXME FIXME Use better approach here, like: Queue steal 
        // request, and try to find a job until some deadline runs out.

        if (b == null) { 
            
            int rnd = selectTargetWorker(); 
    
            b = workers[rnd];
            
            if (Debug.DEBUG_STEAL) { 
                logger.info("M SELECT worker " + rnd + " " + b.identifier() +
                        "for STEAL with context " + sr.context);
            }
        }

        b.deliverStealRequest(sr);
    }

    public void deliverLookupRequest(LookupRequest lr) {

        if (Debug.DEBUG_LOOKUP) { 
            logger.info("M REMOTE LOOKUP REQUEST from " + lr.source);
        }
        
        LocationCache.Entry tmp = locationCache.lookupEntry(lr.missing);

        if (tmp != null) { 
    
            if (Debug.DEBUG_LOOKUP) { 
                logger.info("M sending LOOKUP reply to " + lr.source);
            }
            
            parent.handleLookupReply(new LookupReply(identifier, lr.source, 
                    lr.missing, tmp.id, tmp.count));
            return;
        }

        CohortIdentifier cid = lr.target;

        if (lr.target != null) { 
        
            if (Debug.DEBUG_LOOKUP) { 
                logger.info("M forwarding LOOKUP to " + cid);
            }
            
            if (identifier.equals(cid)) { 
                return;
            }

            BottomCohort b = getWorker(cid);

            if (b != null) { 
                b.deliverLookupRequest(lr);
                return;
            }
        }

        if (Debug.DEBUG_LOOKUP) { 
            logger.info("M forwarding LOOKUP to all children");
        }
        
        for (int i=0;i<workerCount;i++) { 
            workers[i].deliverLookupRequest(lr);
        }
    }

    public void deliverStealReply(StealReply sr) {

        if (Debug.DEBUG_STEAL) { 
            logger.info("M receive STEAL reply from " + sr.source);
        }
        
        System.err.println("MT STEAL reply: " + Arrays.toString(sr.getWork()));
        
        CohortIdentifier cid = sr.target;

        if (cid == null) { 

            if (!sr.isEmpty()) { 
                System.err.println("MT STEAL reply SAVED LOCALLY (1): " + Arrays.toString(sr.getWork()));
                
                myActivities.enqueue(sr.getWork());
                logger.warning("DROP StealReply without target after saving work!");
            } else { 
                logger.warning("DROP empty StealReply without target!");
            }

            return;
        }

        if (identifier.equals(cid)) { 
           
            System.err.println("MT STEAL reply SAVED LOCALLY (2): " + Arrays.toString(sr.getWork()));
            
            if (!sr.isEmpty()) { 
                myActivities.enqueue(sr.getWork());
            }
            return;
        }

        BottomCohort b = getWorker(cid);

        if (cid == null) { 

            if (!sr.isEmpty()) { 
                
                System.err.println("MT STEAL reply SAVED LOCALLY (3): " + Arrays.toString(sr.getWork()));
                
                myActivities.enqueue(sr.getWork());
                logger.warning("DROP StealReply for unknown target after saving work!");
            } else { 
                logger.warning("DROP empty StealReply for unknown target!");
            }

            return;
        }

        b.deliverStealReply(sr);
    }

    public void deliverLookupReply(LookupReply lr) {

        if (Debug.DEBUG_LOOKUP) { 
            logger.info("M received LOOKUP reply from " + lr.source);
        }
        
        // Cache the result!
        locationCache.put(lr.missing, lr.location, lr.count);

        CohortIdentifier cid = lr.target;

        if (cid == null) { 
            logger.warning("Received lookup reply without target!");
            return;
        }

        if (identifier.equals(cid)) { 
            return;
        }

        BottomCohort b = getWorker(cid);

        if (cid == null) { 
            logger.warning("Received event message for unknown target!");
            return;
        }

        b.deliverLookupReply(lr);
    }

    public void deliverEventMessage(ApplicationMessage m) {

        CohortIdentifier cid = m.target;

        if (cid == null) { 
            logger.warning("DROP EventMessage without target!");
            return;
        }

        if (identifier.equals(cid)) { 
            logger.warning("DROP unexpected EventMessage for " + cid);
            return;
        }

        BottomCohort b = getWorker(cid);

        if (cid == null) { 
            logger.warning("DROP EventMessage for unknown target " + cid);
            return;
        }

        b.deliverEventMessage(m);
    }

    public void deliverUndeliverableEvent(UndeliverableEvent m) {
        logger.fixme("DROP UndeliverableEvent", new Exception());
    }

   

    /*

    public void deliverMessage(Message m) {

        CohortIdentifier cid = m.target;

        if (cid == null) { 
            System.out.println("ERROR: received message without target!");
            return;
        }

        BottomCohort b = getWorker(cid);

        if (cid == null) { 
            System.out.println("ERROR: received for unknown target " + cid);
            return;
        }

        b.deliverMessage(m);
    }

     */
    /* ================= End of BottomCohort interface =======================*/


}
