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
import ibis.cohort.extra.CohortIdentifierFactory;
import ibis.cohort.extra.CohortLogger;
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
import java.util.Properties;

public class DistributedCohort implements Cohort, TopCohort {
    
    private static final boolean DEBUG = true;
    private static final boolean PROFILE = true;

    private static boolean REMOTE_STEAL_THROTTLE = false;
    private static long REMOTE_STEAL_TIMEOUT = 1000;

    private final BottomCohort subCohort;

    private final ActivityRecordQueue queue = new ActivityRecordQueue();

    private final CohortIdentifier identifier;

    private final LocationCache cache = new LocationCache();


    private final Pool pool;
      
    private final DistributedCohortIdentifierFactory cidFactory;

    private final CohortLogger logger;
    
    
//  private final Transfer transfer;

//  private final Lookup lookup;
  
//  private final Incoming incoming;

    
    private Context myContext;
    
    private long stealReplyDeadLine;

    private long startID = 0;
    private long blockSize = 1000000;

    
    private boolean pendingSteal = false;

/*
    private class Transfer extends Thread { 
        
        private boolean done = false;
        
        private final CircularBuffer work = new CircularBuffer(16);
            
        public synchronized void done() { 
            done = true;
        }
        
        private synchronized boolean getDone() { 
            return done;
        }
        
        public synchronized void enqueue(Message m) {
            work.insertLast(m);
            notifyAll();
        }
        
        public void start() { 
            
            
            
            
        }
    }
 
    private class Lookup extends Thread { 
        
        private boolean done = false;
        
        private final CircularBuffer work = new CircularBuffer(16);
            
        public synchronized void done() { 
            done = true;
        }
        
        private synchronized boolean getDone() { 
            return done;
        }
        
        public synchronized void enqueue(Message m) {
            work.insertLast(m);
            notifyAll();
        }
        
        public void start() { 
            
            
            
            
        }
    }
 
    
    private class Incoming extends Thread { 
        
        private boolean done = false;
        
        private final CircularBuffer work = new CircularBuffer(16);
        
        public synchronized void done() { 
            done = true;
        }
        
        private synchronized boolean getDone() { 
            return done;
        }
        
        public synchronized void enqueue(Object o) {
            work.insertFirst(o);
            notifyAll();
        }
        
        public void start() { 
        
            
            
            
            
        }
    }
*/

    public DistributedCohort(Properties p) throws Exception {         

        //     if (PROFILE) { 
        //        gcbeans = ManagementFactory.getGarbageCollectorMXBeans();
        //     }

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

        myContext = Context.ANY;

        /*
        lookup = new Lookup();
        lookup.start();
        
        transfer = new Transfer();
        transfer.start();
        
        incoming = new Incoming();
        incoming.start();
        */
        
        // Init communication here...
        pool = new Pool(this, p);

        cidFactory = pool.getCIDFactory();        
        identifier = cidFactory.generateCohortIdentifier();

        // TODO: THIS IS WRONG!!!
        subCohort = new MultiThreadedMiddleCohort(p, this, 0);     
        
        logger = CohortLogger.getLogger(DistributedCohort.class, identifier);
        
        pool.setLogger(logger);        
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
        ActivityRecord ar = queue.steal(sr.context);

        if (ar != null) { 
                
            if (!pool.forward(new StealReply(identifier, sr.source, ar))) { 
                logger.warning("DROP StealReply to " + sr.source);
                queue.add(ar);
                return;
            }
        }
            
        subCohort.deliverStealRequest(sr);
    }
    
    protected void deliverRemoteLookupRequest(LookupRequest lr) { 
        // This method is called from an finished upcall. Therefore it 
        // may block for a long period of time or communicate.
        
        logger.warning("RECEIVED LOOKUP REQUEST " + lr.source + " " 
                + lr.target + " " + lr.missing);
        
        if (lr.target == null || identifier.equals(lr.target)) { 
            // I am the target (including any lower cohorts). This is the normal 
            // case.
        
            logger.warning("I AM TARGET");
                  
            LocationCache.Entry tmp = cache.lookupEntry(lr.missing);
            
            if (tmp != null) { 
                pool.forward(new LookupReply(identifier, lr.source, lr.missing,
                        tmp.id, tmp.count));
                
                // Ignore reply of forward. We don't care if it fails!
                return;
            }
        }
     
        logger.warning("DELIVER TO CHILD");
        
        
        subCohort.deliverLookupRequest(lr);
    }
        
    protected void deliverRemoteStealReply(StealReply sr) { 
        // This method is called from an unfinished upcall. It may NOT 
        // block for a long period of time or communicate!
  
        if (identifier.equals(sr.target)) { 
            if (sr.work != null) { 
                queue.add(sr.work);
            }
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
        boolean tmp = subCohort.activate();
  
        pool.activate();
        
        return tmp;
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
        return new Cohort [] { (Cohort)subCohort };
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

        if (id == null || id.equals(identifier)) { 
            throw new Exception("Cannot set Context of a DistributedCohort!");
        }

        subCohort.setContext(id, context);
    } 

    public ActivityIdentifier submit(Activity a) {
        return subCohort.deliverSubmit(a);
    }

    /* =========== End of Cohort interface ================================== */

    
    
    /* =========== TopCohort interface ====================================== */

    public synchronized void contextChanged(CohortIdentifier cid, Context c) {

        // Sanity check
        if (!cid.equals(subCohort.identifier())) { 
            
            System.err.println("INTERNAL ERROR: Context changed by unknown" +
                    " cohort " + cid);
            return;
        } 

        myContext = c;
    }

    public LookupReply handleLookup(LookupRequest lr) {

        LocationCache.Entry tmp = cache.lookupEntry(lr.missing); 

        if (tmp != null) { 

            if (DEBUG) { 
                logger.info("LOOKUP returns " + tmp.id + " / " + tmp.count);
            }
            
            return new LookupReply(identifier, lr.source, lr.missing, tmp.id, tmp.count);
        }
        
        logger.fixme("BROADCAST LOOKUP: " + lr.missing);
        
        pool.broadcast(lr);
        
        return null;
    }

    public ActivityRecord handleStealRequest(StealRequest sr) {

        // A steal request coming in from the subcohort below. 

        // First check if we can satisfy the request locally.

        if (DEBUG) { 
            logger.info("STEAL REQUEST from child " + sr.source);
        }
        
        ActivityRecord ar = queue.steal(sr.context);

        if (ar != null) { 
       
            if (DEBUG) { 
                logger.info("STEAL REPLY (LOCAL) " + ar.identifier() 
                        + " for child " + sr.source);
            }
           
            return ar;
        }

        // Next, select a random cohort to steal a job from.
        if (pool.randomForward(sr)) { 
      
            if (DEBUG) { 
                logger.info("RANDOM FORWARD steal request from child " 
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
        
        logger.fixme("STEAL REQUEST swizzled from " + sr.source);
                
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

        ActivityIdentifier aid = null;
        long count = -1;
        
        if (m.work != null) { 
            aid = m.work.identifier();
            count = (m.work.getHopCount() + 1);
        }
        
        if (pool.forward(m)) { 
            // Succesfully send the reply. Cache new location of the activity
            if (m.work != null) { 
                cache.put(aid, m.target, count);
            } 
        } else { 
            // Send failed. Reclaim work
            if (m.work != null) {
                logger.warning("DROP StealReply to " + m.target 
                        + " after reclaiming " + aid);
                queue.add(m.work);
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
        queue.add(ar);
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
    /* =========== End of TopCohort interface =============================== */
}
