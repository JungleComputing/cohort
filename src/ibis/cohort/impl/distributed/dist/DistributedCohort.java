package ibis.cohort.impl.distributed.dist;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.ActivityIdentifierFactory;
import ibis.cohort.CancelEvent;
import ibis.cohort.Cohort;
import ibis.cohort.CohortIdentifier;
import ibis.cohort.Context;
import ibis.cohort.Event;
import ibis.cohort.MessageEvent;
import ibis.cohort.context.UnitContext;
import ibis.cohort.extra.CohortIdentifierFactory;
import ibis.cohort.extra.CohortLogger;
import ibis.cohort.extra.Debug;
import ibis.cohort.extra.SmartWorkQueue;
import ibis.cohort.extra.SynchronizedWorkQueue;
import ibis.cohort.extra.WorkQueue;
import ibis.cohort.extra.WorkQueueFactory;
import ibis.cohort.impl.distributed.ActivityRecord;
import ibis.cohort.impl.distributed.ActivityRecordQueue;
import ibis.cohort.impl.distributed.ApplicationMessage;
import ibis.cohort.impl.distributed.BottomCohort;
import ibis.cohort.impl.distributed.LocationCache;
import ibis.cohort.impl.distributed.LookupReply;
import ibis.cohort.impl.distributed.LookupRequest;
import ibis.cohort.impl.distributed.StealReply;
import ibis.cohort.impl.distributed.StealRequest;
import ibis.cohort.impl.distributed.TopCohort;
import ibis.cohort.impl.distributed.UndeliverableEvent;
import ibis.cohort.impl.distributed.multi.MultiThreadedMiddleCohort;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Properties;

public class DistributedCohort implements Cohort, TopCohort {

    private static final boolean PROFILE = true;

    private static boolean REMOTE_STEAL_THROTTLE = false;
    private static long REMOTE_STEAL_TIMEOUT = 1000;
    private static boolean PUSHDOWN_SUBMITS = false;
    
    private boolean active;

    private BottomCohort subCohort;

    private final WorkQueue queue; 
     
    private final CohortIdentifier identifier;

    private final LocationCache cache = new LocationCache();

    private final Pool pool;

    private final DistributedCohortIdentifierFactory cidFactory;

    private final CohortLogger logger;

    private ActivityIdentifierFactory aidFactory;

    private Context myContext;

    private long stealReplyDeadLine;

    private long startID = 0;
    private long blockSize = 1000000;

    private boolean pendingSteal = false;

    public DistributedCohort(Properties p) throws Exception {         

        String tmp = p.getProperty("ibis.cohort.remotesteal.throttle");

        if (tmp != null) {

            try { 
                REMOTE_STEAL_THROTTLE = Boolean.parseBoolean(tmp);
            } catch (Exception e) {
                System.err.println("Failed to parse " +
                        "ibis.cohort.remotesteal.throttle: " + tmp);
            }
        }

        tmp = p.getProperty("ibis.cohort.remotesteal.timeout");

        if (tmp != null) {

            try { 
                REMOTE_STEAL_TIMEOUT = Long.parseLong(tmp);
            } catch (Exception e) {
                System.err.println("Failed to parse " +
                        "ibis.cohort.remotesteal.timeout: " + tmp);
            }
        }

        tmp = p.getProperty("ibis.cohort.submit.pushdown");

        if (tmp != null) {

            try { 
                PUSHDOWN_SUBMITS = Boolean.parseBoolean(tmp);
            } catch (Exception e) {
                System.err.println("Failed to parse " +
                        "ibis.cohort.submits.pushdown: " + tmp);
            }
        }
        
        myContext = UnitContext.DEFAULT;

        // Init communication here...
        pool = new Pool(this, p);

        cidFactory = pool.getCIDFactory();        
        identifier = cidFactory.generateCohortIdentifier();

        tmp = p.getProperty("ibis.cohort.workqueue");
        
        queue = WorkQueueFactory.createQueue(tmp, true, "D(" + identifier + ")");
        
        aidFactory = getActivityIdentifierFactory(identifier);        

        logger = CohortLogger.getLogger(DistributedCohort.class, identifier);

        logger.warn("Starting DistributedCohort " + identifier + " / " + myContext);
    }


    public PrintStream getOutput() {
        return System.out;
    }


    private void printStatistics() { 

        synchronized (System.out) {


            /*
            System.out.println("Messages send     : " + messagesSend);
            System.out.println("           Events : " + eventsSend);
            System.out.println("           Steals : " + stealsSend);
            System.out.println("             Work : " + workSend);
            System.out.println("          No work : " + no_workSend);
            System.out.println("Messages received : " + messagesReceived);
            System.out.println("           Events : " + eventsReceived);
            System.out.println("           Steals : " + stealsReceived);
            System.out.println("             Work : " + workReceived);
            System.out.println("          No work : " + no_workReceived);
             */
            if (PROFILE) { 
                /*
                System.out.println("GC beans     : " + gcbeans.size());

                for (GarbageCollectorMXBean gc : gcbeans) { 
                    System.out.println(" GC bean : " + gc.getName());
                    System.out.println("   count : " + gc.getCollectionCount());
                    System.out.println("   time  : " + gc.getCollectionTime());
                }
                 */
            }
        }
    }


    private synchronized boolean setPendingSteal(boolean value) { 

        // When we are setting the value to false, we don't care about 
        // the deadline. 
        if (!value) { 
            boolean tmp = pendingSteal; 
            pendingSteal = false;
            stealReplyDeadLine = 0;
            return tmp;
        } 

        long time = System.currentTimeMillis();

        // When we are changing the value from false to true, we also
        // need to set te deadline.
        if (!pendingSteal) { 
            pendingSteal = true;
            stealReplyDeadLine = time + REMOTE_STEAL_TIMEOUT;
            return false;
        }

        // When the old value was true but the deadline has passed, we act as 
        // if the value was false to begin with
        if (time > stealReplyDeadLine) { 
            pendingSteal = true;
            stealReplyDeadLine = time + REMOTE_STEAL_TIMEOUT;
            return false;
        }

        // Otherwise, we leave the value and deadline unchanged
        return true;    
    }




    /*
    private void queueOutgoingMessage(Message m) { 

        if (m.isTargetSet()) { 
            transfer.enqueue(m);

        } else if (m.requiresLookup()) { 
            lookup.enqueue(m);

        } else if (m.requiresRandomSelection()) { 

            CohortIdentifier cid = pool.selectTarget();

            if (cid == null) { 
                System.err.println("INTERNAL ERROR: failed to randomly select "
                        + " a target for message " + m);
                new Exception().printStackTrace(System.err);

                return;
            }

            m.setTarget(cid);
            transfer.enqueue(m);

        } else { 
            System.err.println("INTERNAL ERROR: Do not know how to handle " +
                        "message " + m);
            new Exception().printStackTrace(System.err);
        }
    }

    protected void queueIncomingWork(Object o) { 
        incoming.enqueue(o);
    }
     */

    private boolean isLocal(CohortIdentifier id) { 
        return pool.isLocal(id);
    }

    /* =========== Callback interface for Pool ============================== */

    protected void deliverRemoteStealRequest(StealRequest sr) { 
        // This method is called from an finished upcall. Therefore it 
        // may block for a long period of time or communicate.

        if (Debug.DEBUG_STEAL) { 
            logger.info("D REMOTE STEAL REQUEST from child " + sr.source 
                    + " context " + sr.context);
        }

     //   System.out.println("DIST REMOTE STEAL " + sr.context + " from " + sr.source);     
        
        ActivityRecord ar = queue.steal(sr.context, true);

        if (ar != null) { 

        //    System.out.println("DIST REMOTE STEAL RESTURNS " 
        //            + ar.activity.getContext() + " " + ar.identifier());     
            
            if (Debug.DEBUG_STEAL) {
                logger.info("D LOCAL REPLY for STEAL REQUEST from child " 
                        + sr.source + " context " + sr.context);
            }

            if (!pool.forward(new StealReply(identifier, sr.source, ar))) { 
                logger.warning("DROP StealReply to " + sr.source);
                queue.enqueue(ar);
            } 

            return;
        } else {
            if (Debug.DEBUG_STEAL) {
                logger.info("D No local reply for STEAL REQUEST from child " 
                        + sr.source + " context " + sr.context);
            }
        }

        subCohort.deliverStealRequest(sr);
    }

    protected void deliverRemoteLookupRequest(LookupRequest lr) { 
        // This method is called from an finished upcall. Therefore it 
        // may block for a long period of time or communicate.

        if (Debug.DEBUG_LOOKUP) { 
            logger.warning("RECEIVED LOOKUP REQUEST " + lr.source + " " 
                    + lr.target + " " + lr.missing);
        }

        if (lr.target == null || identifier.equals(lr.target)) { 
            // I am the target (including any lower cohorts). This is the normal 
            // case.

            if (Debug.DEBUG_LOOKUP) { 
                logger.warning("I AM TARGET");
            }

            LocationCache.Entry tmp = cache.lookupEntry(lr.missing);

            if (tmp != null) { 
                pool.forward(new LookupReply(identifier, lr.source, lr.missing,
                        tmp.id, tmp.count));

                // Ignore reply of forward. We don't care if it fails!
                return;
            }
        }

        if (Debug.DEBUG_LOOKUP) {
            logger.warning("DELIVER TO CHILD");
        }

        subCohort.deliverLookupRequest(lr);
    }

    protected void deliverRemoteStealReply(StealReply sr) { 
        // This method is called from an unfinished upcall. It may NOT 
        // block for a long period of time or communicate!

        System.err.println("DIST STEAL reply: " + Arrays.toString(sr.getWork()));

        if (identifier.equals(sr.target)) { 
            if (!sr.isEmpty()) { 
                queue.enqueue(sr.getWork());
            }
            return;
        }

        subCohort.deliverStealReply(sr);
    }


    protected void deliverRemoteLookupReply(LookupReply lr) { 
        // This method is called from an unfinished upcall. It may NOT 
        // block for a long period of time or communicate!

        cache.put(lr.missing, lr.location, lr.count);

        if (identifier.equals(lr.target)) { 
            return;
        }

        subCohort.deliverLookupReply(lr);
    }


    protected void deliverRemoteEvent(ApplicationMessage re) { 
        // This method is called from an unfinished upcall. It may NOT 
        // block for a long period of time or communicate!

        if (identifier.equals(re.target)) { 
            logger.warning("DROP unexpected EventMessage " + re.event);
            return;
        } 

        subCohort.deliverEventMessage(re);
    }

    protected void deliverUndeliverableEvent(UndeliverableEvent ue) { 
        // This method is called from an unfinished upcall. It may NOT 
        // block for a long period of time or communicate!

        if (identifier.equals(ue.target)) { 
            logger.warning("DROP unexpected UndeliverableEvent " + ue.event);
            return;
        } 

        subCohort.deliverUndeliverableEvent(ue);
    }


    /* =========== End of Callback interface for Pool ======================= */



    /* =========== Cohort interface ========================================= */

    public boolean activate() {

        synchronized (this) {
            active = true;
        }

        pool.activate();
        return subCohort.activate();
    }

    public void cancel(ActivityIdentifier id) {
        send(new CancelEvent(id));
    }

    public boolean deregister(String name, Context scope) {
        // TODO Auto-generated method stub
        return false;
    }

    public void done() {
        try { 
            // NOTE: this will proceed directly on the master. On other 
            // instances, it blocks until the master terminates. 
            pool.terminate();
        } catch (Exception e) {
            logger.warning("Failed to terminate pool!", e);
        }

        subCohort.done();        

        printStatistics();

        pool.cleanup();
    }

    public Cohort [] getSubCohorts() {

        if (subCohort instanceof Cohort) {
            return new Cohort [] { (Cohort)subCohort };
        } else { 
            return null;
        }
    } 

    public CohortIdentifier [] getLeafIDs() {
        return subCohort.getLeafIDs();
    }

    public CohortIdentifier identifier() {
        return identifier;
    }

    public boolean isMaster() {
        return pool.isMaster();
    }

    public ActivityIdentifier lookup(String name, Context scope) {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean register(String name, ActivityIdentifier id, Context scope) {
        // TODO Auto-generated method stub
        return false;
    }

    public void send(ActivityIdentifier source, ActivityIdentifier target, 
            Object o) {

        send(new MessageEvent(source, target, o));
    }

    public void send(Event e) {

        // This is triggered as a result of a user sending a message (top down).
        // We therefore do not know if the target cohort is local or remote. 

        ApplicationMessage tmp = new ApplicationMessage(identifier, e);
        CohortIdentifier cid = cache.lookup(e.target);

        if (cid != null) { 
            tmp.setTarget(cid);

            if (isLocal(cid)) { 
                subCohort.deliverEventMessage(tmp);
            } else { 
                pool.forward(tmp);
            }

            return;
        }

        enqueue(tmp);
    }

    public synchronized Context getContext() {
        return myContext;
    }


    public void setContext(Context context) throws Exception {
        setContext(null, context);
    }

    public synchronized void setContext(CohortIdentifier id, Context context) 
    throws Exception {

        if (Debug.DEBUG_CONTEXT) { 
            logger.info("Setting context of " + id + " to " + context);
        }

        if (id == null || id.equals(identifier)) { 
            throw new Exception("Cannot set Context of a DistributedCohort!");
        }

        subCohort.setContext(id, context);
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

    public ActivityIdentifier submit(Activity a) {

        if (PUSHDOWN_SUBMITS) { 

            if (Debug.DEBUG_SUBMIT) { 
                logger.info("D PUSHDOWN SUBMIT activity with context " + a.getContext());
            }

            return subCohort.deliverSubmit(a);
        }
   
        if (Debug.DEBUG_SUBMIT) { 
            logger.info("D LOCAL SUBMIT activity with context " + a.getContext());
        }
        
        ActivityIdentifier id = createActivityID();
        a.initialize(id);
        
        queue.enqueue(new ActivityRecord(a));
        
        if (Debug.DEBUG_SUBMIT) {
            logger.info("created " + id + " at " 
                    + System.currentTimeMillis() + " from DIST"); 
        }

        System.out.println("DIST -- LOCAL ENQ: " + id + " " + a.getContext() + " " + a.getRank());
        
        return id;
    }
    

    /* =========== End of Cohort interface ================================== */



    /* =========== TopCohort interface ====================================== */

    public synchronized void contextChanged(CohortIdentifier cid, Context c) {

        // Sanity check
        if (!cid.equals(subCohort.identifier())) { 
            logger.warning("INTERNAL ERROR: Context changed by unknown" +
                    " cohort " + cid);
            return;
        } 

        myContext = c;
    }

    public LookupReply handleLookup(LookupRequest lr) {

        LocationCache.Entry tmp = cache.lookupEntry(lr.missing); 

        if (tmp != null) { 

            if (Debug.DEBUG_LOOKUP) { 
                logger.info("LOOKUP returns " + tmp.id + " / " + tmp.count);
            }

            return new LookupReply(identifier, lr.source, lr.missing, tmp.id, tmp.count);
        }

        logger.fixme("BROADCAST LOOKUP: " + lr.missing);

        pool.broadcast(lr);

        return null;
    }

    public ActivityRecord handleStealRequest(StealRequest sr) {

       // System.out.println("DIST STEAL");

        // A steal request coming in from the subcohort below. 

        // First check if we can satisfy the request locally.
        if (Debug.DEBUG_STEAL) { 
            logger.info("D STEAL REQUEST from child " + sr.source);
        }

  // System.out.println("DIST LOCAL STEAL " + sr.context + " from " + sr.source);     
        
        ActivityRecord ar = queue.steal(sr.context, false);
        
        if (ar != null) { 

            if (Debug.DEBUG_STEAL) { 
                logger.info("D STEAL REPLY (LOCAL) " + ar.identifier() 
                        + " for child " + sr.source);
            }

  // System.out.println("DIST LOCAL STEAL RETURNS " + ar.activity.getContext() + " " +  ar.identifier());     
            
            return ar;
        }


//  System.out.println("DIST STEAL FORWARD");     

        
        // Next, select a random cohort to steal a job from.
        if (pool.randomForward(sr)) { 

            if (Debug.DEBUG_STEAL) { 
                logger.info("D RANDOM FORWARD steal request from child " 
                        + sr.source);
            }

            return null;
        }

        /*
        CohortIdentifier target = pool.selectTarget();

        if (target != null) { 
            StealRequest sr2 = new StealRequest(sr.source, sr.context);
            sr2.setTarget(target);
            pool.forward(sr2);

            System.out.println(identifier + ": Send steal request to " + target);
            return null;
        }
         */


        /* FIXME!

        boolean pending = setPendingSteal(true);

        if (pending) { 
            // Steal request was already pending, so ignore this one.
            return null;
        }

        queueOutgoingMessage(sr);
         */

        logger.fixme("D STEAL REQUEST swizzled from " + sr.source);

        return null;
    }

    private void enqueue(ApplicationMessage m) { 
        logger.fixme("ENQUEUE NOT IMPLEMENTED");
    }

    public void handleApplicationMessage(ApplicationMessage m) { 

        // This is triggered as a result of a (sub)cohort sending a message 
        // (bottom up). We therefore assume the message will leave this machine. 

        if (!m.isTargetSet()) {
            // Try to set the target cohort. 
            m.setTarget(cache.lookup(m.targetActivity())); 
        } 

        if (m.isTargetSet()) { 
            if (pool.forward(m)) { 
                return;
            } else { 
                // Failed to send message. Assume the target is invalid, reset
                // it to null, and retry.
                m.setTarget(null);
            }
        }  

        // Failed to send the message, so queue it. 
        enqueue(m);
    }

    public void handleLookupReply(LookupReply m) { 

        if (m.location != null) { 
            cache.put(m.missing, m.location, m.count);
        }

        pool.forward(m);
    }

    public void handleStealReply(StealReply m) {

        logger.warn("D handling steal reply for " + m.target);

        if (pool.forward(m)) { 
            // Succesfully send the reply. Cache new location of the activities

            if (!m.isEmpty()) { 

                int size = m.getSize();

                for (int i=0;i<size;i++) {
                    ActivityRecord tmp = m.getWork(i);

                    if (tmp != null) { 
                        cache.put(tmp.identifier(), m.target, tmp.getHopCount()+1);
                    }
                }
            }

        } else { 
            // Send failed. Reclaim work
            if (!m.isEmpty()) {
                logger.warning("DROP StealReply to " + m.target 
                        + " after reclaiming work");
                queue.enqueue(m.getWork());
            } else { 
                logger.warning("DROP Empty StealReply to " + m.target); 
            }
        }        
    }

    public void handleUndeliverableEvent(UndeliverableEvent m) {
        logger.fixme("DROP undeliverable event for " + m.event.target);
    }

    public void handleWrongContext(ActivityRecord ar) {
        // Store the 'unusable' activities at this level.
        queue.enqueue(ar);
    }

    public synchronized ActivityIdentifierFactory 
        getActivityIdentifierFactory(CohortIdentifier cid) {

        ActivityIdentifierFactory tmp = new ActivityIdentifierFactory(
                cid.id,  startID, startID+blockSize);

        startID += blockSize;
        return tmp;
    }

    public CohortIdentifierFactory getCohortIdentifierFactory(
            CohortIdentifier cid) {
        return cidFactory;
    }

    public synchronized void register(BottomCohort cohort) throws Exception { 

        if (active || subCohort != null) { 
            throw new Exception("Cannot register BottomCohort");
        }

        subCohort = cohort;
    }

    /* =========== End of TopCohort interface =============================== */
}
