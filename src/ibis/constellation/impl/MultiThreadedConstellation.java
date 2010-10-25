package ibis.constellation.impl;

import ibis.constellation.Activity;
import ibis.constellation.ActivityIdentifier;
import ibis.constellation.ActivityIdentifierFactory;
import ibis.constellation.ConstellationIdentifier;
import ibis.constellation.Event;
import ibis.constellation.StealPool;
import ibis.constellation.WorkerContext;
import ibis.constellation.context.OrWorkerContext;
import ibis.constellation.context.UnitWorkerContext;
import ibis.constellation.extra.ActivityLocationCache;
import ibis.constellation.extra.ActivityLocationLookup;
import ibis.constellation.extra.CircularBuffer;
import ibis.constellation.extra.ConstellationIdentifierFactory;
import ibis.constellation.extra.ConstellationLogger;
import ibis.constellation.extra.Debug;
import ibis.constellation.extra.StealPoolInfo;
import ibis.constellation.extra.WorkQueue;
import ibis.constellation.extra.WorkQueueFactory;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

//FIXME: crap name!
public class MultiThreadedConstellation {

    private static boolean PUSHDOWN_SUBMITS = false;

    private final DistributedConstellation parent;

    private ArrayList<SingleThreadedConstellation> incomingWorkers;

    private SingleThreadedConstellation [] workers;
    
    private StealPool belongsTo;
    private StealPool stealsFrom;
    
    private int workerCount;
    
    private final ConstellationIdentifier identifier;

    private final Random random = new Random();

    private boolean active = false;

    private WorkerContext [] contexts;
    private WorkerContext myContext;
    private boolean myContextChanged = false;

    // private ActivityRecordQueue myActivities = new ActivityRecordQueue();

    private final WorkQueue queue;
    private final WorkQueue restrictedQueue;
    
    // private LocationCache locationCache = new LocationCache();
    
    private final ActivityLocationLookup exportedActivities; 
    private final ActivityLocationLookup importedActivities;     
    private final ActivityLocationCache remoteActivities; 
    
    private final ConstellationIdentifierFactory cidFactory;

    private ActivityIdentifierFactory aidFactory;    
    private long startID = 0;
    private long blockSize = 1000000;
    
    private int nextSubmit = 0;

    //private final LookupThread lookup;

    private final ConstellationLogger logger;
    
    private final StealPoolInfo poolInfo = new StealPoolInfo();
        
    /*
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

            ConstellationIdentifier cid = locationCache.lookup(t);

            if (cid != null) { 

                m.setTarget(cid);

                BottomConstellation b = getWorker(cid);

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
   */

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

    public MultiThreadedConstellation(DistributedConstellation parent, Properties p) throws Exception {

        int count = 0;

        this.parent = parent;

        remoteActivities = parent.getRemoteActivityCache();
        exportedActivities = parent.getExportedActivityLookup();
        importedActivities = parent.getImportedActivityLookup();
        
        cidFactory = parent.getConstellationIdentifierFactory(null);
        identifier = parent.identifier();
        
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

        queue = parent.getQueue();
        restrictedQueue = parent.getRestrictedQueue();
        
        this.logger = ConstellationLogger.getLogger(MultiThreadedConstellation.class, identifier);

        incomingWorkers = new ArrayList<SingleThreadedConstellation>();
        //     localSteals = new StealState[count];
        contexts    = new WorkerContext[count];

        myContext = UnitWorkerContext.DEFAULT;
        myContextChanged = false;

        /*
        lookup = new LookupThread();
        lookup.start();
         */
        
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

    // NOTE: only for use by sub constellations
    //public ActivityLocationCache getRemoteActivityCache() { 
    //	return remoteActivities;
   // }

    protected ActivityLocationLookup getExportedActivityLookup() { 
    	return exportedActivities;
    }

    //public ActivityLocationLookup getImportedActivityLookup() { 
    //	return importedActivities;
    //}
    // END OF NOTE 
    
    private SingleThreadedConstellation getWorker(ConstellationIdentifier cid) { 

        for (SingleThreadedConstellation b : workers) { 
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

    /*
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
*/
    
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

    public synchronized void contextChanged(ConstellationIdentifier cid, WorkerContext newContext) {

        for (int i=0;i<workerCount;i++) { 

            if (cid.equals(workers[i].identifier())) {
                contexts[i] = newContext;
                myContextChanged = true;
                return;
            }
        }

        logger.warning("Cohort " + cid + " not found!");
    }

    public synchronized ActivityIdentifierFactory getActivityIdentifierFactory(ConstellationIdentifier cid) {

    	ActivityIdentifierFactory tmp = new ActivityIdentifierFactory(
    			cid.id,  startID, startID+blockSize);

    	startID += blockSize;
    	return tmp;
    }    
    
    private synchronized ActivityIdentifier createActivityID(boolean events) {

        try {
            return aidFactory.createActivityID(events);
        } catch (Exception e) {
            // Oops, we ran out of IDs. Get some more from our parent!
            aidFactory = getActivityIdentifierFactory(identifier);
        }

        try {
            return aidFactory.createActivityID(events);
        } catch (Exception e) {
            throw new RuntimeException(
                    "INTERNAL ERROR: failed to create new ID block!", e);
        }
    }
    
    public ActivityIdentifier submit(Activity a) {
        
        if (Debug.DEBUG_SUBMIT) { 
            logger.info("LOCAL SUBMIT activity with context " + a.getContext());
        }

        ActivityIdentifier id = createActivityID(a.expectsEvents());
        a.initialize(id);

        // Add externally submitted activities to the lookup table. 
        exportedActivities.add(id, identifier);
        
        if (a.isRestrictedToLocal()) { 
            restrictedQueue.enqueue(new ActivityRecord(a));
        } else { 
            queue.enqueue(new ActivityRecord(a));
        }

        if (Debug.DEBUG_SUBMIT) {
            logger.info("created " + id + " at " + System.currentTimeMillis()); 
        }

        System.out.println("LOCAL ENQ: " + id + " " + a.getContext());

        return id;
    }

    public void reclaim(ActivityRecord [] ar) { 

    	if (ar == null || ar.length == 0) {
    		return;
    	}

    	for (int i=0;i<ar.length;i++) { 
    		ActivityRecord a = ar[i];

    		if (a != null) {     			
    			// FIXME: there is a nasty race condition here!
    			// 
    			// We have registered these activities as exported to some remote 
    			// machine, only to reclaim them a moment later. In this moment, 
    			// however, someone may have done a lookup and send an event to the 
    			// (unknowing) new location.
    			//
    			// Similarly, when we overwrite the export to point to this constellation, 
    			// there is a short moment at which the activity is registered in exported, 
    			// but not in one of the queues....
    			// 
    			// Not sure what the effect will be if this is triggered.
    			
    			exportedActivities.add(a.identifier(), identifier);
    			
    			if (ar[i].isRestrictedToLocal()) {
    				logger.error("INTERNAL ERROR: Reclaimed RESTRICTED activity from dicarded StealReply!!");
    				restrictedQueue.enqueue(ar[i]);
    			} else { 
    				queue.enqueue(ar[i]);
    			}    			
            }
        } 
    }


/*
    public LookupReply handleLookup(LookupRequest lr) {

        // Check if the activity location is cached locally     
        LocationCache.Entry e = locationCache.lookupEntry(lr.missing);

        if (e != null) { 
            return new LookupReply(identifier, lr.source, lr.missing, e.id, e.count);
        }

        // Check if the activity is in my queue 
        if (restrictedQueue.contains(lr.missing) || queue.contains(lr.missing)) { 
        	return new LookupReply(identifier, lr.source, lr.missing, identifier, 0);            
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
*/   
    private boolean handleEventLocally(ConstellationIdentifier src, ConstellationIdentifier location, ActivityIdentifier target, Event e) { 
    	
		if (location.equals(identifier)) {
			// Case 1a/2c It should be in one of my queues 
			boolean delivered = restrictedQueue.deliver(target, e) || queue.deliver(target, e); 

			if (delivered) { 
				return true;
			}

			// Intentional fall-through  
		} 
		
		SingleThreadedConstellation worker = getWorker(location);
		
		if (worker != null) {
			// Case 1b or 2a/b: activity is located in one of our workers    				
			worker.deliverEventMessage(new ApplicationMessage(src, location, e));
			return true;
		}

		return false;
    } 
    
/*
    public void handleEvent(ConstellationIdentifier src, ConstellationIdentifier dst, Event e) { 

    	if (Debug.DEBUG_EVENTS) { 
            logger.info("Event for " + e.target + " at " + dst);
        }

    	if (handleEventLocally(src, dst, e)) { 
    		return;
    	}

    	// Destination is not local. Let our parent handle this.
    	parent.handleApplicationMessage(new ApplicationMessage(src, dst, e));
    }
*/
    
    public void handleEvent(ConstellationIdentifier src, Event e) { 
    	
    	// We must now forward 'e' to its destination 'e.target'.
    	// 
    	// We known that 'e' is not local to 'src', but we don't know its current location. 
    	// There are several option now: 
    	//
    	//
    	// 1) 'src' is the origin of 'e.target' but it is no longer there because:
    	//   a) 'src' has pushed away 'e.target', and it it now in our queue 
    	//   b) 'e.target' is stolen by a local executor 
    	//   c) 'e.target' is stolen by a remote executor (may still be in transit)  
     	//   d) 'e.target' has terminated 
        //
    	// 2) 'e.target' has originated somewhere else in this constellation and 
    	//   a) is still at its origin
    	//   b) is stolen by some other local executor
    	//   c) is pushed away to our queue
    	//   d) 'e.target' is stolen by a remote executor (may still be in transit)  
    	//   e) 'e.target' has terminated  
    	//
    	// 3) 'e.target' has a remote origin but was imported into this constellation and 
        //   a) is in our queue.
    	//   b) is located in some other local executor
    	//   c) has terminated  
    	//
    	// 4) 'e.target' has a remote origin and has always been remote and 
    	//   a) is located in some remote queue
    	//   b) has terminated 
    	//
        // In all cases 'e.target' can tell us in which constellation the activity originated. 
   
    	final ActivityIdentifier target = e.target;        	
    	final ConstellationIdentifier origin = e.target.getOrigin();
    	ConstellationIdentifier location = null;    
    	
    	if (cidFactory.isLocal(origin)) { 
        	
    		// Case 1 or 2: The target originated in our local constellation.    		
    		location = exportedActivities.lookup(target);

    		if (location == null) {
    			// Case 1d/2e: The target no longer exists. This should simply be an application bug ? 
    			// FIXME: be sure ! 
    			logger.error("ERROR: cannot locate activity " + target + " (activity may have terminated!)");    			
    			return;
    		}
    		
    		if (cidFactory.isLocal(location)) {     		
    			// Case 1b or 2a/b    			
    			if (!handleEventLocally(src, location, target, e)) { 
    				logger.warn("WARNING: cannot locate activity " + e.target + "(not in expected location -- will retry)");    			
    				enqueue(e, src);	
    			}
    			
				return;
    		}
    			
    		// Case 1c/2d: location is remote
    		parent.handleApplicationMessage(new ApplicationMessage(src, location, e));
    		return;
    	}
    	
    	// Case 3 or 4: the activity has a remote origin
    	location = importedActivities.lookup(target);
    	
    	if (location != null) { 
    		// Case 3a/b: the activity has a remote origin and is now located on this constellation
    		if (!handleEventLocally(src, location, target, e)) { 
				logger.warn("WARNING: cannot locate activity " + e.target + "(not in expected location -- will retry)");    			
				enqueue(e, src);	
			}
			
			return;
    	}

    	// Case 3c or 4a/b: the activity has a remote origin and may have terminated already        
    	location = remoteActivities.lookup(target);
    	
    	if (location == null) {
    		// If we cannot find the location of the activity, we simply send the event to the 
    		// constellation where the activity originated
    		//
    		// FIXME: alternative is sending a lookup request to the parent and enqueue the 
    		// event until we have an answer
    		location = origin;
    	} 
    	
    	parent.handleApplicationMessage(new ApplicationMessage(src, location, e));
    }
    
	private void enqueue(Event e, ConstellationIdentifier src) {
		logger.error("INTERAL ERROR: enqueue of event not implemented yet! (1) FIX THIS NOW YOU TWAT!!");    					
	}
    
    /*
    public void handleApplicationMessage(ApplicationMessage m) {

        if (Debug.DEBUG_EVENTS) { 
            logger.info("EventMessage for " + m.event.target + " at " + m.target);
        }

        if (m.isTargetSet()) { 

            BottomConstellation b = getWorker(m.target);

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
     */
/*    
    public void handleLookupReply(LookupReply m) { 
        // Cache locally!
        locationCache.put(m.missing, m.location, m.count);                   

        if (m.isTargetSet()) { 

            ConstellationIdentifier id = m.target;

            if (id.equals(identifier)) { 
                return;
            }

            BottomConstellation b = getWorker(m.target);

            if (b != null) {
                b.deliverLookupReply(m);
                return;
            }

            parent.handleLookupReply(m);
            return;
        } 

        logger.warning("Dropping lookup reply! " + m);        
    }
*/
	
    public void handleStealReply(StealReply m) { 

       // logger.warn("MT handling STEAL REPLY " + m.isTargetSet() + " " + m.isEmpty());

        if (m.isTargetSet()) { 

            SingleThreadedConstellation b = getWorker(m.target);

            if (b != null) { 

            //    logger.warn("MT handling STEAL REPLY target is local! " + m.target);

                b.deliverStealReply(m);
                return;
            }

           // logger.warn("MT handling STEAL REPLY target is remote! " + m.target);

            parent.handleStealReply(m);
        } else {
            // Should never happen ?
            if (!m.isEmpty()) { 
                logger.warning("Saving work before dropping StealReply");
                
                ActivityRecord [] ar = m.getWork();
                
                // Sanity check
                for (int i=0;i<ar.length;i++) { 
                	if (ar[i].isRestrictedToLocal()) { 
                		System.out.println("INTERNAL ERROR: Saving RESTRICTED work to queue!");
                   		logger.warn("INTERNAL ERROR: Saving RESTRICTED work to queue!");
                   }
                }
                
                queue.enqueue(m.getWork());
            } else { 
                logger.warn("Dropping empty StealReply");            
            }
        }
    }

    public void handleUndeliverableEvent(UndeliverableEvent m) {
        // FIXME FIX!
        logger.fixme("I just dropped an undeliverable event! " + m.event);
    }

    private int selectTargetWorker(ConstellationIdentifier exclude) { 

        if (workerCount == 1) { 
            return -1;
        }

        int rnd = selectTargetWorker();

        ConstellationIdentifier cid = workers[rnd].identifier();

        if (cid.equals(exclude)) { 
            return ((rnd+1)%workerCount);
        }

        return rnd;        
    }

    public ActivityRecord handleStealRequest(StealRequest sr) {

    	// FXIME: This currently ignores the pool information in the steal request!
    	
        if (Debug.DEBUG_STEAL) { 
            logger.info("M STEAL REQUEST from child " + sr.source + " with context " 
                    + sr.context);
        }

        // Try the restricted queue first 
        ActivityRecord tmp = restrictedQueue.steal(sr.context);

        if (tmp != null) { 

            if (Debug.DEBUG_STEAL) { 
                logger.info("M STEAL REPLY (LOCAL-RESTRICTED) " + tmp.identifier() 
                        + " for child " + sr.source);
            }
            return tmp;
        }
        
        // Try the normal queue next 
        tmp = queue.steal(sr.context);

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

        	// FIXME: hard coded steal size!
            StealRequest copy = new StealRequest(sr.source, sr.context, sr.pool, 1);

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
        
        if (ar.isRestrictedToLocal()) { 
            restrictedQueue.enqueue(ar);
        } else { 
            queue.enqueue(ar);
        }
        
        
        System.out.println("MT has " + restrictedQueue.size() + " + " + queue.size() + " activities queued!");
    }

    public ConstellationIdentifierFactory getCohortIdentifierFactory(ConstellationIdentifier cid) {
        return parent.getConstellationIdentifierFactory(cid);        
    }

    public synchronized void register(SingleThreadedConstellation cohort) throws Exception {

        if (active) { 
            throw new Exception("Cannot register new BottomCohort while " +
            "TopCohort is active!");
        }

        incomingWorkers.add(cohort);        
    }


    /* ================= End of TopCohort interface ==========================*/





    /* ================= BottomCohort interface ==============================*/
/*
    public ConstellationIdentifier [] getLeafIDs() { 

        ArrayList<ConstellationIdentifier> tmp = new ArrayList<ConstellationIdentifier>();

        for (BottomConstellation w : workers) { 
            ConstellationIdentifier [] ids = w.getLeafIDs();

            for (ConstellationIdentifier id : ids) { 
                tmp.add(id);
            }
        }

        return tmp.toArray(new ConstellationIdentifier[tmp.size()]);
    }

    public synchronized void setContext(ConstellationIdentifier id, WorkerContext context) throws Exception {

        if (Debug.DEBUG_CONTEXT) { 
            logger.info("Setting context of " + id + " to " + context);
        }

        BottomConstellation b = getWorker(id);

        if (b == null) {
            throw new Exception("Cohort " + id + " not found!");
        } 

        b.setContext(id, context);
    }
*/
    
    public synchronized WorkerContext getContext() {

        if (!myContextChanged) { 
            return myContext;
        } 

        // We should now combine all contexts of our workers into one
        HashMap<String, UnitWorkerContext> map = 
            new HashMap<String, UnitWorkerContext>();

        for (int i=0;i<workerCount;i++) {

            WorkerContext tmp = workers[i].getContext();

            if (tmp.isUnit()) { 

                UnitWorkerContext u = (UnitWorkerContext) tmp;

                String tag = u.uniqueTag();

                if (!map.containsKey(tag)) { 
                    map.put(tag, u);
                }
            } else if (tmp.isOr()) { 
                OrWorkerContext o = (OrWorkerContext) tmp;

                for (int j=0;j<o.size();j++) { 
                    UnitWorkerContext u = o.get(i);

                    String tag = u.uniqueTag();

                    if (!map.containsKey(tag)) { 
                        map.put(tag, u);
                    }
                }
            }
        }

        if (map.size() == 0) { 
            // FIXME should not happen ?
            myContext = UnitWorkerContext.DEFAULT;
        } else if (map.size() == 1) { 
            myContext = contexts[1];
        } else { 
            UnitWorkerContext [] contexts = map.values().toArray(new UnitWorkerContext[map.size()]);                
            myContext = new OrWorkerContext(contexts, false); 
        }

        myContextChanged = false;
        return myContext;
    }

    public ConstellationIdentifier identifier() {
        return identifier;
    }

    public boolean activate() { 

        synchronized (this) {
            if (active) { 
                return false;
            }

            active = true;
        
            workerCount = incomingWorkers.size();
            workers = incomingWorkers.toArray(new SingleThreadedConstellation[workerCount]);

            StealPool [] tmp = new StealPool[workerCount];
           
            for (int i = 0; i < workerCount; i++) {
                tmp[i] = workers[i].belongsTo();
            }
            
            belongsTo = StealPool.merge(tmp);
            
            for (int i = 0; i < workerCount; i++) {
                tmp[i] = workers[i].stealsFrom();
            }
            
            stealsFrom = StealPool.merge(tmp);
              
            // No workers may be added after this point
            incomingWorkers = null;
        } 
         
        if (parent != null) { 
        	parent.belongsTo(belongsTo);
        	parent.stealsFrom(stealsFrom);
        }
        
        for (int i = 0; i < workerCount; i++) {
            logger.info("Activating worker " + i);
            workers[i].activate();
        }

        return true;
    }

    public void done() {

        logger.warn("done");

      //  lookup.done();

        if (active) { 
            for (SingleThreadedConstellation u : workers) {
                u.done();
            }
        } else { 
            for (SingleThreadedConstellation u : incomingWorkers) {
                u.done();
            }
        }
    }

    public boolean canProcessActivities() {
        return false;
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

        ActivityIdentifier id = createActivityID(a.expectsEvents());
        a.initialize(id);

        if (Debug.DEBUG_SUBMIT) {
            logger.info("created " + id + " at " 
                    + System.currentTimeMillis() + " from DIST"); 
        }

        return id;
    }

    public void deliverStealRequest(StealRequest sr) {

        if (Debug.DEBUG_STEAL) { 
            logger.info("M REMOTE STEAL REQUEST from child " + sr.source 
                    + " context " + sr.context);
        }

        ActivityRecord [] tmp = queue.steal(sr.context, sr.size);

        if (tmp != null && tmp.length > 0) { 

            if (Debug.DEBUG_STEAL) { 
                logger.info("M SUCCESFUL REMOTE STEAL REQUEST from child " + sr.source 
                        + " context " + sr.context);
            }
            
            // Register activities as being exported
            for (int i=0;i<tmp.length;i++) { 
            	
            	ActivityRecord ar = tmp[i];
            	
            	if (ar != null) { 
            		exportedActivities.add(ar.identifier(), sr.source);
            	}
            }
            
            parent.handleStealReply(new StealReply(identifier, sr.source, tmp));
            return;
        }

        // We couldn't satisfy the steal request locally, so we forward to a sub constellation.
        ConstellationIdentifier cid = sr.target;
        SingleThreadedConstellation b = null;

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

/*    
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
        
        ConstellationIdentifier cid = lr.target;

        if (lr.target != null) { 

            if (Debug.DEBUG_LOOKUP) { 
                logger.info("M forwarding LOOKUP to " + cid);
            }

            if (identifier.equals(cid)) { 
                return;
            }

            BottomConstellation b = getWorker(cid);

            if (b != null) { 
                b.deliverLookupRequest(lr);
                return;
            }
        }

        // Check if the activity is in my queue 
        if (restrictedQueue.contains(lr.missing) || queue.contains(lr.missing)) { 
        	parent.handleLookupReply(new LookupReply(identifier, lr.source, lr.missing, identifier, 0));
        	return;
        }
        
        if (Debug.DEBUG_LOOKUP) { 
            logger.info("M forwarding LOOKUP to all children");
        }

        for (int i=0;i<workerCount;i++) { 
            workers[i].deliverLookupRequest(lr);
        }
    }
*/
    public void deliverStealReply(StealReply sr) {

        if (Debug.DEBUG_STEAL) { 
            logger.info("M receive STEAL reply from " + sr.source);
        }

      //  System.err.println("MT STEAL reply: " + Arrays.toString(sr.getWork()));

        ConstellationIdentifier cid = sr.target;

        if (cid == null) { 

            if (!sr.isEmpty()) { 
                System.err.println("MT STEAL reply SAVED LOCALLY (1): " + Arrays.toString(sr.getWork()));

                queue.enqueue(sr.getWork());
                logger.warning("DROP StealReply without target after saving work!");
            } else { 
                logger.warning("DROP empty StealReply without target!");
            }

            return;
        }

        if (identifier.equals(cid)) { 

            System.err.println("MT STEAL reply SAVED LOCALLY (2): " + Arrays.toString(sr.getWork()));

            if (!sr.isEmpty()) { 
                queue.enqueue(sr.getWork());
            }
            return;
        }

        SingleThreadedConstellation b = getWorker(cid);

        if (b == null) { 

            if (!sr.isEmpty()) { 

                System.err.println("MT STEAL reply SAVED LOCALLY (3): " + Arrays.toString(sr.getWork()));

                queue.enqueue(sr.getWork());
                logger.warning("DROP StealReply for unknown target after saving work!");
            } else { 
                logger.warning("DROP empty StealReply for unknown target!");
            }

            return;
        }

        b.deliverStealReply(sr);
    }
/*
    public void deliverLookupReply(LookupReply lr) {

        if (Debug.DEBUG_LOOKUP) { 
            logger.info("M received LOOKUP reply from " + lr.source);
        }

        // Cache the result!
        locationCache.put(lr.missing, lr.location, lr.count);

        ConstellationIdentifier cid = lr.target;

        if (cid == null) { 
            logger.warning("Received lookup reply without target!");
            return;
        }

        if (identifier.equals(cid)) { 
            return;
        }

        BottomConstellation b = getWorker(cid);

        if (cid == null) { 
            logger.warning("Received event message for unknown target!");
            return;
        }

        b.deliverLookupReply(lr);
    }
*/
    public void deliverEventMessage(Event e, ConstellationIdentifier source, ConstellationIdentifier target) {

    	ActivityIdentifier aid = e.target;
    	
    	// The target activity should be known in exportedActivities or importedActivities    	
    	ConstellationIdentifier location = exportedActivities.lookup(aid);
    	
    	if (location == null) { 
    		location = importedActivities.lookup(aid);
    	} 
    	
    	if (location == null) { 
    		logger.error("ERROR: failed to locate target activity " + aid + " for remote event (dropping event)");
    		return;
    	} 
    	
    	if (!handleEventLocally(source, location, aid, e)) { 
    		logger.error("ERROR: failed to deliver event to activity " + aid + "(dropping event)");
        }
    }

    public void deliverUndeliverableEvent(UndeliverableEvent m) {
        logger.fixme("DROP UndeliverableEvent", new Exception());
    }

/*    
	public void addToLookupCache(ConstellationIdentifier cid, ActivityIdentifier aid) {
		locationCache.put(aid, cid, 0);
	}
*/
    /*
    
	@Override
	public void registerPool(
			SingleThreadedBottomCohort singleThreadedBottomCohort,
			StealPool oldPool, StealPool newPool) {

		// TODO: We should maintain a list here of all pools we know (and their participants).
		
		// 1) Register locally. 
		
		// 2) Create a join of all tags 
		
		HashSet<String> tags = 
		
		
		// 2) Register with parent
		
		
		
		
		
		
		
	}

	@Override
	public void registerStealPool(
			SingleThreadedBottomCohort singleThreadedBottomCohort,
			StealPool oldPool, StealPool newPool) {
		
		// Do we need this one ? Shouldn't we just let the STCohort tell us which pool it 
		// wants to steal from ? Also, when throttling steal request, we should do this on a 
		// per pool basis ?
		
		// TODO Auto-generated method stub
		
	}
*/
    
    /* ================= End of BottomCohort interface =======================*/


}
