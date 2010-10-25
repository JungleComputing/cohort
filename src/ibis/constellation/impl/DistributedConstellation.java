package ibis.constellation.impl;

import ibis.constellation.Activity;
import ibis.constellation.ActivityIdentifier;
import ibis.constellation.CancelEvent;
import ibis.constellation.Constellation;
import ibis.constellation.ConstellationIdentifier;
import ibis.constellation.Event;
import ibis.constellation.MessageEvent;
import ibis.constellation.StealPool;
import ibis.constellation.WorkerContext;
import ibis.constellation.context.UnitWorkerContext;
import ibis.constellation.extra.ActivityLocationCache;
import ibis.constellation.extra.ActivityLocationLookup;
import ibis.constellation.extra.ConstellationIdentifierFactory;
import ibis.constellation.extra.ConstellationLogger;
import ibis.constellation.extra.Debug;
import ibis.constellation.extra.WorkQueue;
import ibis.constellation.extra.WorkQueueFactory;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Properties;

public class DistributedConstellation implements Constellation {

	private static final int STEAL_POOL   = 1; 
	private static final int STEAL_MASTER = 2;
    private static final int STEAL_NONE   = 3;
    
    private static final boolean PROFILE = true;

    private static final int REMOTE_ACTIVITY_CACHE = 5000;
    
    private static boolean REMOTE_STEAL_THROTTLE = true;

    // FIXME setting this to low at startup causes load imbalance!
    //    machines keep hammering the master for work, and (after a while) 
    //    get a flood of replies. 
    private static long REMOTE_STEAL_TIMEOUT = 60000;

    private static boolean PUSHDOWN_SUBMITS = false;

    private boolean active;

    private MultiThreadedConstellation subConstellation;

    private final WorkQueue queue; 
    private final WorkQueue restrictedQueue; 

    private final ConstellationIdentifier identifier;

    // private final LocationCache cache = new LocationCache();

    private final ActivityLocationLookup exportedActivities = new ActivityLocationLookup(); 
    private final ActivityLocationLookup importedActivities = new ActivityLocationLookup();     
    private final ActivityLocationCache remoteActivities; 
    
    private final Pool pool;

    private final DistributedConstellationIdentifierFactory cidFactory;

    private final ConstellationLogger logger;

    private WorkerContext myContext;

    private long stealReplyDeadLine;

    private boolean pendingSteal = false;

    private final int stealing; 

    private final long start;

    public DistributedConstellation(Properties p) throws Exception {         

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

        String stealName = p.getProperty("ibis.cohort.stealing", "pool");

        /*if (stealName.equalsIgnoreCase("random")) { 
            stealing = STEAL_RANDOM;
        } else*/
        if (stealName.equalsIgnoreCase("mw")) {
            stealing = STEAL_MASTER;
        } else if (stealName.equalsIgnoreCase("none")) {
            stealing = STEAL_NONE;
        } else if (stealName.equalsIgnoreCase("pool")) { 
            stealing = STEAL_POOL;
        } else { 
            System.err.println("Unknown stealing strategy: " + stealName);
            throw new Exception("Unknown stealing strategy: " + stealName);
        }

        myContext = UnitWorkerContext.DEFAULT;

        
        int cacheSize = Integer.parseInt(p.getProperty("ibis.cohort.	remote_activity_cache", "" + REMOTE_ACTIVITY_CACHE));

        remoteActivities = new ActivityLocationCache(cacheSize);
        
        // Init communication here...
        pool = new Pool(this, p);

        cidFactory = pool.getCIDFactory();        
        identifier = cidFactory.generateConstellationIdentifier();

        String queueName = p.getProperty("ibis.cohort.workqueue");

        queue = WorkQueueFactory.createQueue(queueName, true, 
                "D(" + identifier.id + ")");

        restrictedQueue = WorkQueueFactory.createQueue(queueName, true, 
                "D(" + identifier.id + "-RESTRICTED)");

        logger = ConstellationLogger.getLogger(DistributedConstellation.class, identifier);

        start = System.currentTimeMillis();

        if (true) { 
            System.out.println("DistributeConstellation : " + identifier.id);
            System.out.println("               throttle : " + REMOTE_STEAL_THROTTLE);
            System.out.println("         throttle delay : " + REMOTE_STEAL_TIMEOUT);
            System.out.println("               pushdown : " + PUSHDOWN_SUBMITS);
            System.out.println("                  queue : " + queueName);     
            System.out.println("               stealing : " + stealName);
            System.out.println("         location cache : " + cacheSize);
            System.out.println("                  start : " + start);
            
        }

        logger.warn("Starting DistributedConstellation " + identifier + " / " + myContext);
    }

    protected ActivityLocationCache getRemoteActivityCache() { 
    	return remoteActivities;
    }

    protected ActivityLocationLookup getExportedActivityLookup() { 
    	return exportedActivities;
    }

    protected ActivityLocationLookup getImportedActivityLookup() { 
    	return importedActivities;
    }
    
    protected WorkQueue getRestrictedQueue() { 
    	return restrictedQueue;
    }
    
    protected WorkQueue getQueue() { 
    	return queue;
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
        // need to set the deadline.
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
  
    private boolean isLocal(ConstellationIdentifier id) { 
        return pool.isLocal(id);
    }
*/
    /* =========== Callback interface for Pool ============================== */

    protected void deliverRemoteStealRequest(StealRequest sr) { 
        // This method is called from an finished upcall. Therefore it 
        // may block for a long period of time or communicate.

        if (Debug.DEBUG_STEAL) { 
            logger.info("D REMOTE STEAL REQUEST from cohort " + sr.source 
                    + " context " + sr.context);
        }

        //   System.out.println("DIST REMOTE STEAL " + sr.context + " from " + sr.source);     
/*
        // NOTE: only allowed to steal from 'normal' queue
        ActivityRecord ar = queue.steal(sr.context);

        if (ar != null) { 

            //    System.out.println("DIST REMOTE STEAL RESTURNS " 
            //            + ar.activity.getContext() + " " + ar.identifier());     

            if (Debug.DEBUG_STEAL) {
                logger.info("D REMOTE REPLY for STEAL REQUEST from cohort " 
                        + sr.source + " context " + sr.context + " " 
                        + ar.identifier() + " " + ar.activity.getContext());
            }

            System.out.println((System.currentTimeMillis()-start) + " D REMOTE REPLY for STEAL REQUEST from cohort " 
                    + sr.source + " context " + sr.context + " " 
                    + ar.identifier() + " " + ar.activity.getContext()); 

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

            System.out.println((System.currentTimeMillis()-start) + " D NO REPLY for STEAL REQUEST from cohort " 
                    + sr.source + " context " + sr.context); 

        }
*/
        
        subConstellation.deliverStealRequest(sr);
    }

/*    
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

            // Check if the activity is in my queue 
            if (restrictedQueue.contains(lr.missing) || queue.contains(lr.missing)) { 
                pool.forward(new LookupReply(identifier, lr.source, lr.missing, identifier, 0));            
            }
        }

        if (Debug.DEBUG_LOOKUP) {
            logger.warning("DELIVER TO CHILD");
        }

        subConstellation.deliverLookupRequest(lr);
    }
*/
    protected void deliverRemoteStealReply(StealReply sr) { 
        // This method is called from an unfinished upcall. It may NOT 
        // block for a long period of time or communicate!

        setPendingSteal(false);
/*
        System.err.println("DIST STEAL reply: " + Arrays.toString(sr.getWork()));

        if (identifier.equals(sr.target)) { 
            if (!sr.isEmpty()) {
                // NOTE: should never get restricted work!
                queue.enqueue(sr.getWork());
            }
            return;
        }
*/        
        subConstellation.deliverStealReply(sr);
    }

/*
    protected void deliverRemoteLookupReply(LookupReply lr) { 
        // This method is called from an unfinished upcall. It may NOT 
        // block for a long period of time or communicate!

        cache.put(lr.missing, lr.location, lr.count);

        if (identifier.equals(lr.target)) { 
            return;
        }

        subConstellation.deliverLookupReply(lr);
    }
*/
    
/*
    private boolean deliverEventToLocalActivity(Event e) { 

        ActivityIdentifier id = e.target;

        ActivityRecord a = restrictedQueue.lookup(id);

        if (a == null) { 
            a = queue.lookup(id);
        }

        if (a != null) {
            synchronized (a) { 
                a.enqueue(e);
            }
            return true;
        }

        return false;
    }
*/
    
    protected void deliverRemoteEvent(ApplicationMessage re) { 
        // This method is called from an unfinished upcall. It may NOT 
        // block for a long period of time or communicate!   
    	subConstellation.deliverEventMessage(re.event, re.source, re.target);
    }
    
   /* protected void deliverUndeliverableEvent(UndeliverableEvent ue) { 
        // This method is called from an unfinished upcall. It may NOT 
        // block for a long period of time or communicate!

        if (identifier.equals(ue.target)) { 
            logger.warning("DROP unexpected UndeliverableEvent " + ue.event);
            return;
        } 

        subConstellation.deliverUndeliverableEvent(ue);
    }
*/

    /* =========== End of Callback interface for Pool ======================= */



    /* =========== Cohort interface ========================================= */

    public boolean activate() {

        synchronized (this) {
            active = true;
        }

        pool.activate();
        return subConstellation.activate();
    }

    public void cancel(ActivityIdentifier id) {
        send(new CancelEvent(id));
    }

/*    
    public boolean deregister(String name, ActivityContext scope) {
        // TODO DOES THIS MAKE SENSE ?
        return false;
    }
*/
    
    public void done() {
        try { 
            // NOTE: this will proceed directly on the master. On other 
            // instances, it blocks until the master terminates. 
            pool.terminate();
        } catch (Exception e) {
            logger.warning("Failed to terminate pool!", e);
        }

        subConstellation.done();        

        printStatistics();

        pool.cleanup();
    }

/*
    public Constellation [] getSubCohorts() {

        if (subConstellation instanceof Constellation) {
            return new Constellation [] { (Constellation)subConstellation };
        } else { 
            return null;
        }
    } 

    public ConstellationIdentifier [] getLeafIDs() {
        return subConstellation.getLeafIDs();
    }
*/
    
    public ConstellationIdentifier identifier() {
        return identifier;
    }

    public boolean isMaster() {
        return pool.isMaster();
    }

    /*
    public ActivityIdentifier lookup(String name, ActivityContext scope) {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean register(String name, ActivityIdentifier id, ActivityContext scope) {
        // TODO Auto-generated method stub
        return false;
    }
     */
    
    public void send(ActivityIdentifier source, ActivityIdentifier target, Object o) {
        send(new MessageEvent(source, target, o));
    }
    
    public void send(Event e) {
    	
    	if (!e.target.expectsEvents) { 
    		throw new IllegalArgumentException("Target activity " + e.target + "  does not expect an event!");
    	}
    	
    	// An external application wishes to send an event to 'e.target'. 
    	// Let our sub constellation handle this
    	subConstellation.handleEvent(identifier, e);
    }

    public synchronized WorkerContext getContext() {
        return myContext;
    }

/*   
    public void setContext(WorkerContext context) throws Exception {
        setContext(null, context);
    }

    
    public synchronized void setContext(ConstellationIdentifier id, WorkerContext context) throws Exception {

        if (Debug.DEBUG_CONTEXT) { 
            logger.info("Setting context of " + id + " to " + context);
        }

        if (id == null || id.equals(identifier)) { 
            throw new Exception("Cannot set Context of a DistributedConstellation!");
        }

        subConstellation.setContext(id, context);
    } 
*/    

   

    public ActivityIdentifier submit(Activity a) {
    	return subConstellation.submit(a);
    }
    


    /* =========== End of Cohort interface ================================== */



    /* =========== TopCohort interface ====================================== */

    public synchronized void contextChanged(ConstellationIdentifier cid, WorkerContext c) {

        // Sanity check
        if (!cid.equals(subConstellation.identifier())) { 
            logger.warning("INTERNAL ERROR: Context changed by unknown" +
                    " cohort " + cid);
            return;
        } 

        myContext = c;
    }

/*
    public LookupReply handleLookup(LookupRequest lr) {

        LocationCache.Entry tmp = cache.lookupEntry(lr.missing); 

        if (tmp != null) { 

            if (Debug.DEBUG_LOOKUP) { 
                logger.info("LOOKUP returns " + tmp.id + " / " + tmp.count);
            }

            return new LookupReply(identifier, lr.source, lr.missing, tmp.id, tmp.count);
        }

        // The missing activity could be in my queue ? 
        if (restrictedQueue.contains(lr.missing) || queue.contains(lr.missing)) { 
            return new LookupReply(identifier, lr.source, lr.missing, identifier, 0);            
        }

        logger.fixme("BROADCAST LOOKUP: " + lr.missing);

        pool.broadcast(lr);

        return null;
    }
*/
    
    public ActivityRecord handleStealRequest(StealRequest sr) {

        // A steal request coming in from the subcohort below. 

    	if (stealing == STEAL_NONE) {
            logger.debug("D STEAL REQUEST swizzled from " + sr.source);
            return null;
        }
        
        if (stealing == STEAL_MASTER) {

            if (pool.isMaster()) {
                // Master does not steal from itself!
                return null;
            }

        	if (REMOTE_STEAL_THROTTLE) { 

                boolean pending = setPendingSteal(true);

                if (pending) { 
                    // We have already send out a steal in this slot, so 
                    // we're not allowed to send another one.
                    return null;
                }
            }

            if (pool.forwardToMaster(sr)) { 

                if (Debug.DEBUG_STEAL) { 
                    logger.info("D MASTER FORWARD steal request from child " 
                            + sr.source);
                }
            }

            return null;
        } 

                
        if (stealing == STEAL_POOL) { 

            if (sr.pool == null || sr.pool.isNone()) { 
                // Stealing from nobody is easy!
                return null;
            }

            // TODO: make throttling pool aware ? 
            if (REMOTE_STEAL_THROTTLE) { 

                boolean pending = setPendingSteal(true);

                if (pending) { 

                    //System.out.println("POOL steal is already pending");

                    // We have already send out a steal in this slot, so 
                    // we're not allowed to send another one.
                    return null;
                }
            }

            if (pool.randomForwardToPool(sr)) { 

                if (Debug.DEBUG_STEAL) { 
                    logger.info("D RANDOM FORWARD steal request from child " 
                            + sr.source  + " to POOL " + sr.pool.getTag());
                }
            }

            return null;

        } 
        
        logger.fixme("D STEAL REQUEST unknown stealing strategy " + stealing);

        return null;
    }
    
/*
    private void enqueue(ApplicationMessage m) { 
        logger.error("INTERNAL ERROR: ");
    }
    
    public void handleApplicationMessage(ApplicationMessage m) { 

        // This is triggered as a result of a (sub)cohort sending a message 
        // (bottom up). 

        if (!m.isTargetSet()) {
            // Try to set the target cohort. 
            m.setTarget(cache.lookup(m.targetActivity())); 
        } 

        if (m.isTargetSet()) {

            if (identifier.equals(m.target)) { 

                // The message is headed for one of the queued activities
                if (deliverEventToLocalActivity(m.event)) {
                    return;
                }

                // FIXME: This is a likely BUG!
                logger.fixme("FAILED TO DELIVER application message from " + m.event.source + " to " + m.event.target);
            } else if (pool.forward(m)) { 
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
*/

    public void handleApplicationMessage(ApplicationMessage m) { 

        // This is triggered as a result of someone in our constellation sending 
    	// a message (bottom up). 

    	ConstellationIdentifier target = m.target;
    	
    	// Sanity check
    	if (cidFactory.isLocal(target)) { 
    		logger.error("INTERNAL ERROR: received message for local constellation (dropped message!)");
    		return;
    	}
    	
    	if (pool.forward(m)) { 
    		return;
    	} 
    
		logger.error("ERROR: failed to forward message to remote constellation " + target + " (dropped message!)");
    }

/*    
    public void handleLookupReply(LookupReply m) { 

        if (m.location != null) { 
            cache.put(m.missing, m.location, m.count);
        }

        pool.forward(m);
    }
*/
    
    public void handleStealReply(StealReply m) {

    	// Handle a steal reply (bottom up)
    	
    	ConstellationIdentifier target = m.target;
    	
    	// Sanity check
    	if (cidFactory.isLocal(target)) { 
    		logger.error("INTERNAL ERROR: received steal reply for local constellation (reclaiming work and dropped reply)");
    		subConstellation.reclaim(m.getWork());
        	return;
    	}
    	
        if (!pool.forward(m)) {
            // If the send fails we reclaim the work.
        	
        	if (!m.isEmpty()) { 
        	    logger.warn("FAILED to deliver steal reply to " + target + " (reclaiming work and dropping reply)");        		
        		subConstellation.reclaim(m.getWork());
        	} else { 
                logger.warn("FAILED to deliver empty steal reply to " + target + " (dropping reply)"); 
        	}               
        }
    }
    
    //public void handleUndeliverableEvent(UndeliverableEvent m) {
    //    logger.fixme("DROP undeliverable event for " + m.event.target);
    //}

/*    
    public void handleWrongContext(ActivityRecord ar) {
        // Store the 'unusable' activities at this level.
        if (ar.isRestrictedToLocal()) { 
            restrictedQueue.enqueue(ar);
        } else { 
            queue.enqueue(ar);
        }
    }
*/
  
    public ConstellationIdentifierFactory getConstellationIdentifierFactory(
            ConstellationIdentifier cid) {
        return cidFactory;
    }

    public synchronized void register(MultiThreadedConstellation c) throws Exception { 

        if (active || subConstellation != null) { 
            throw new Exception("Cannot register BottomConstellation");
        }

        subConstellation = c;
    }


    //public void registerWithPool(String tag) {
    //	pool.registerWithPool(tag);
    //}

    // TODO: add unreg to pool
    //public void registerInterestInPool(String tag) {
    //	pool.followPool(tag);
    // }


    public void belongsTo(StealPool belongsTo) {

        if (belongsTo == null) { 
            logger.error("Constellation does not belong to any pool!");
            return;
        }

        if (belongsTo.isNone()) { 
            // We don't belong to any pool. As a result, no one can steal from us.  
            return;
        }

        if (belongsTo.isSet()) { 

            StealPool [] set = belongsTo.set();

            for (int i=0;i<set.length;i++) { 

                if (!set[i].isWorld()) { 
                    pool.registerWithPool(set[i].getTag());
                }
            }

        } else { 
            if (!belongsTo.isWorld()) { 
                pool.registerWithPool(belongsTo.getTag());
            }
        }
    }

    public void stealsFrom(StealPool stealsFrom) {

        if (stealsFrom == null) { 
            logger.warn("Constellation does not steal from to any pool!");
            return;
        }

        if (stealsFrom.isNone()) { 
            // We don't belong to any pool. As a result, no one can steal from us.  
            return;
        }

        if (stealsFrom.isSet()) { 

            StealPool [] set = stealsFrom.set();

            for (int i=0;i<set.length;i++) { 
                pool.followPool(set[i].getTag());
            }

        } else { 
            pool.followPool(stealsFrom.getTag());
        }
    }

    /* =========== End of TopCohort interface =============================== */
}
