package ibis.cohort.impl.distributed.multi;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.ActivityIdentifierFactory;
import ibis.cohort.CohortIdentifier;
import ibis.cohort.Context;
import ibis.cohort.context.AndContext;
import ibis.cohort.context.ContextSet;
import ibis.cohort.context.UnitContext;
import ibis.cohort.extra.CircularBuffer;
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
import ibis.cohort.impl.distributed.single.SingleThreadedBottomCohort;

import java.io.PrintStream;
import java.util.Properties;
import java.util.Random;

//FIXME: crap name!
public class MultiThreadedMiddleCohort implements TopCohort, BottomCohort {

    private static final boolean DEBUG = true;
    
//    private static final int REMOTE_STEAL_RANDOM = 0;
//    private static final int REMOTE_STEAL_ALL    = 1;

//    private static final int REMOTE_STEAL_POLICY = 
//        determineRemoteStealPolicy(REMOTE_STEAL_RANDOM);
    
    private final TopCohort parent;
    
    private final BottomCohort [] workers;

    private final CohortIdentifier identifier;

    private final Random random = new Random();

    private final int workerCount;

    private boolean active = false;
    
    private Context [] contexts;
    private Context myContext;
    private boolean myContextChanged = false;
    
    private ActivityRecordQueue myActivities = new ActivityRecordQueue();

    private LocationCache locationCache = new LocationCache();
    
    private final CohortIdentifierFactory cidFactory;
   
    private int nextSubmit = 0;
    
    private final LookupThread lookup;
    
    private final CohortLogger logger;
    
    private class LookupThread extends Thread { 
        
        // NOTE: should use exp. backoff here!
        
        private static final int TIMEOUT = 100; // 1 sec. timeout. 

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
        
        private synchronized void waitForTimeout() { 
            
            try { 
                wait(TIMEOUT);
            } catch (InterruptedException e) {
                // ignored
            }                
        }
        
        private synchronized boolean getDone() { 
            return done;
        }
     
        public synchronized void done() { 
            done = true;
        }
        
        public void run() { 

            while (!getDone()) {
                waitForTimeout();
                int oldM = processOldMessages();                    
                int newM = addNewMessages();

                if ((oldM + newM) > 0) {
                    if (DEBUG) { 
                        logger.info("LOOKUP: " + oldM + " " + newM + " " 
                            + oldMessages.size());
                    }
                }
            }
            
            if (DEBUG) { 
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

    public MultiThreadedMiddleCohort(Properties p, TopCohort parent, 
            int workerCount) {
        
        this.parent = parent;
        
        cidFactory = parent.getCohortIdentifierFactory(null);
        identifier = cidFactory.generateCohortIdentifier();
        
        int count = workerCount;
        
        if (count == 0) {

            String tmp = p.getProperty("ibis.cohort.workers");

            if (tmp != null && tmp.length() > 0) {
                try {
                    count = Integer.parseInt(tmp);
                } catch (Exception e) {
                    System.err.println("Failed to parse property " +
                            "ibis.cohort.workers: " + e);
                }
            }

            if (count == 0) {
                // Automatically determine the number of cores to use
                count = Runtime.getRuntime().availableProcessors();
            }
        }

        this.workerCount = count;

        this.logger = CohortLogger.getLogger(MultiThreadedMiddleCohort.class, identifier);
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
        
        if (DEBUG) { 
            logger.info("EventMessage for " + m.event.target + " at " + m.target);
        }
        
        if (m.isTargetSet()) { 
            
            BottomCohort b = getWorker(m.target);
            
            if (b != null) { 
                
                if (DEBUG) { 
                    logger.info("DELIVERED " + "EventMessage to " + b.identifier()); 
                }
                b.deliverEventMessage(m);
                return;
            }
            
            parent.handleApplicationMessage(m);
            return;
        } 
       
        if (DEBUG) { 
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

        if (m.isTargetSet()) { 
            
            BottomCohort b = getWorker(m.target);
            
            if (b != null) { 
                b.deliverStealReply(m);
                return;
            }
            
            parent.handleStealReply(m);
        } else {
            // Should never happen ?
            if (m.work != null) { 
                logger.warning("Saving work before dropping StealReply: " + m.work.identifier());
                myActivities.add(m.work);
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

        if (DEBUG) { 
            logger.info("STEAL REQUEST from child " + sr.source + " with context " 
                    + sr.context);
        }
        
        ActivityRecord tmp = myActivities.steal(sr.context);
        
        if (tmp != null) { 
        
            if (DEBUG) { 
                logger.info("STEAL REPLY (LOCAL) " + tmp.identifier() 
                        + " for child " + sr.source);
            }
            return tmp;
        }

        tmp = parent.handleStealRequest(sr);
        
        if (tmp != null) {
            logger.info("STEAL REPLY (PARENT) " + tmp.identifier() 
                    + " for child " + sr.source);
            return tmp;
        }   
                
        int index = selectTargetWorker(sr.source);

        if (index >= 0) { 

            StealRequest copy = new StealRequest(sr.source, sr.context);
            
            if (DEBUG) { 
                logger.info("FORWARD STEAL from child " + sr.source 
                        + " to child " + workers[index].identifier());            
            }
                
            copy.setTarget(workers[index].identifier());
            workers[index].deliverStealRequest(copy);
        }
        
        if (tmp != null) {
            logger.info("STEAL REPLY (EMPTY) for child " + sr.source);
        }   
        
        return null;
    }
    
    public void handleWrongContext(ActivityRecord ar) {
        myActivities.add(ar);
    }
    
    public CohortIdentifierFactory getCohortIdentifierFactory(
            CohortIdentifier cid) {
        return parent.getCohortIdentifierFactory(cid);        
    }
    
    /* ================= End of TopCohort interface ==========================*/
    

    
    
    
    /* ================= BottomCohort interface ==============================*/
    
    public synchronized void setContext(CohortIdentifier id, Context context) 
        throws Exception {
    
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
    
    public CohortIdentifier identifier() {
        return identifier;
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
    
    public void done() {
        
        lookup.done();
        
        for (BottomCohort u : workers) {
            u.done();
        }
    }
    
    public boolean canProcessActivities() {
        return false;
    }
        
    
    public synchronized ActivityIdentifier deliverSubmit(Activity a) {

        // We do a simple round-robin distribution of the jobs here.
        if (nextSubmit >= workers.length) {
            nextSubmit = 0;
        }

        if (DEBUG) { 
            logger.info("FORWARD SUBMIT to child " 
                + workers[nextSubmit].identifier());
        }
        
        return workers[nextSubmit++].deliverSubmit(a);
    }
    
    public void deliverStealRequest(StealRequest sr) {

        ActivityRecord tmp = myActivities.steal(sr.context);
        
        if (tmp != null) { 
            parent.handleStealReply(new StealReply(identifier, sr.source, tmp));
            return;
        }
        
        CohortIdentifier cid = sr.target;
        BottomCohort b = null;
          
        if (cid != null) { 
            b = getWorker(cid);
        } 
      
        // If we do not have a specific worker to steal from we'll just pick 
        // one at random...
        
        // FIXME FIXME FIXME Use better approach here, like: Queue steal 
        // request, and try to find a job until some deadline runs out.
        
        if (b == null) { 
            b = workers[selectTargetWorker()];
        }
        
        b.deliverStealRequest(sr);
    }
        
    public void deliverLookupRequest(LookupRequest lr) {
 
        LocationCache.Entry tmp = locationCache.lookupEntry(lr.missing);
        
        if (tmp != null) { 
            parent.handleLookupReply(new LookupReply(identifier, lr.source, 
                    lr.missing, tmp.id, tmp.count));
            return;
        }
        
        CohortIdentifier cid = lr.target;
        
        if (lr.target != null ) { 
            if (identifier.equals(cid)) { 
                return;
            }
        
            BottomCohort b = getWorker(cid);
        
            if (b != null) { 
                b.deliverLookupRequest(lr);
                return;
            }
        }
        
        for (int i=0;i<workerCount;i++) { 
            workers[i].deliverLookupRequest(lr);
        }
    }
    
    public void deliverStealReply(StealReply sr) {
    
        CohortIdentifier cid = sr.target;
        
        if (cid == null) { 
          
            if (sr.work != null) { 
                myActivities.add(sr.work);
                logger.warning("DROP StealReply without target after saving work!");
            } else { 
                logger.warning("DROP empty StealReply without target!");
            }
            
            return;
        }
        
        if (identifier.equals(cid)) { 
            if (sr.work != null) { 
                myActivities.add(sr.work);
            }
            return;
        }
        
        BottomCohort b = getWorker(cid);
        
        if (cid == null) { 
            
            if (sr.work != null) { 
                myActivities.add(sr.work);
                logger.warning("DROP StealReply for unknown target after saving work!");
            } else { 
                logger.warning("DROP empty StealReply for unknown target!");
            }
            
            return;
        }
        
        b.deliverStealReply(sr);
    }
   
    public void deliverLookupReply(LookupReply lr) {
    
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
