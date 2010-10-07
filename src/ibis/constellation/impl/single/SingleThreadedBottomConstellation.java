package ibis.constellation.impl.single;

import ibis.constellation.Activity;
import ibis.constellation.ActivityContext;
import ibis.constellation.ActivityIdentifier;
import ibis.constellation.ActivityIdentifierFactory;
import ibis.constellation.ConstellationIdentifier;
import ibis.constellation.Event;
import ibis.constellation.Executor;
import ibis.constellation.StealPool;
import ibis.constellation.WorkerContext;
import ibis.constellation.extra.CircularBuffer;
import ibis.constellation.extra.ConstellationLogger;
import ibis.constellation.extra.Debug;
import ibis.constellation.impl.ActivityRecord;
import ibis.constellation.impl.ApplicationMessage;
import ibis.constellation.impl.BottomConstellation;
import ibis.constellation.impl.LookupReply;
import ibis.constellation.impl.LookupRequest;
import ibis.constellation.impl.StealReply;
import ibis.constellation.impl.StealRequest;
import ibis.constellation.impl.UndeliverableEvent;
import ibis.constellation.impl.multi.MultiThreadedMiddleConstellation;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

public class SingleThreadedBottomConstellation extends Thread implements BottomConstellation {

    private static final boolean PROFILE = true;
    private static final boolean THROTTLE_STEALS = true;
    private static final int DEFAULT_STEAL_DELAY = 500;
    private static final boolean DEFAULT_IGNORE_EMPTY_STEAL_REPLIES = false;
    
    private final MultiThreadedMiddleConstellation parent;

    private final ExecutorWrapper sequential;

    private final ConstellationIdentifier identifier;
    
    private PrintStream out; 
    private ConstellationLogger logger;
    
    private final Thread thread;
    
    private StealPool myPool;
    private StealPool stealPool;
    
    private boolean poolIsFixed;
    private boolean stealIsFixed;

    private static class PendingRequests {
        
        // These are the new submits 
        final ArrayList<Activity> pendingSubmit = 
            new ArrayList<Activity>();
        
        final ArrayList<ActivityRecord> deliveredActivityRecords = 
            new ArrayList<ActivityRecord>();
       
     //   final ArrayList<Event> pendingEvents = new ArrayList<Event>();

        final ArrayList<ApplicationMessage> deliveredApplicationMessages = 
            new ArrayList<ApplicationMessage>();

        final ArrayList<ActivityIdentifier> pendingCancelations = 
            new ArrayList<ActivityIdentifier>();

        /*
        final ArrayList<StealRequest> stealRequests = 
            new ArrayList<StealRequest>();
        
        final ArrayList<LookupRequest> lookupRequests = 
            new ArrayList<LookupRequest>();
        */
        
        final HashMap<ConstellationIdentifier, StealRequest> stealRequests = 
            new HashMap<ConstellationIdentifier, StealRequest>();
        
        final HashMap<ConstellationIdentifier, LookupRequest> lookupRequests = 
            new HashMap<ConstellationIdentifier, LookupRequest>();
        
        
        WorkerContext newContext;
        
        boolean cancelAll = false;
        
        public String print() { 
            return "QUEUES: " + pendingSubmit.size() + " " 
                    + deliveredActivityRecords.size() + " " 
                    + deliveredApplicationMessages.size() + " "
                    + pendingCancelations.size() + " "  
                    + stealRequests.size() + " "  
                    + lookupRequests.size() + " " 
                    + newContext;
        }
    }
    
    private final int stealSize; 
    private final int stealDelay; 
    
    private long nextStealDeadline; 
    
    private PendingRequests incoming = new PendingRequests();
    private PendingRequests processing = new PendingRequests();

    private boolean done = false;

    private boolean idle = false;
        
    private long eventTime;
    private long activeTime;
    private long idleTime;
    private long idleCount;
    
    // NOTE: these are use for performance debugging...
    /*
    private long profileDelta = 5000;
    private long profileTime = 0;
    private long profileDeadline;
    private long profileComputation = 0;
    private long profileCPU = 0;
    private long profileUser = 0;
    private long profileBlockedC = 0;
    private long profileBlockedT = 0;
    private long profileWaitC = 0;
    private long profileWaitT = 0;
    private long profileSubmit = 0;
    private long profileInvoke = 0;
    private long profileMessageI = 0;
    private long profileMessageE = 0;
    private long profileSteals = 0;
     */
    
    private final boolean ignoreEmptyStealReplies;
    
    private volatile boolean havePendingRequests = false;
    
    public SingleThreadedBottomConstellation(MultiThreadedMiddleConstellation parent, Executor executor, Properties p) 
    	throws Exception {

        super();
        
        this.thread = this;
        this.parent = parent;
        this.identifier = parent.getCohortIdentifierFactory(null).generateConstellationIdentifier();
            
        super.setName("SingleThreadedBottomCohort " + identifier.id);
        
        String outfile = p.getProperty("ibis.cohort.outputfile");
        
        if (outfile != null) {
            String filename = outfile + "." + identifier.id;
            
            try {
                out = new PrintStream(new BufferedOutputStream(
                        new FileOutputStream(filename)));
            } catch (Exception e) {
                System.out.println("Failed to open output file " + outfile);
                out = System.out;
            }
            
        } else { 
            out = System.out;
        }
        
        this.logger = ConstellationLogger.getLogger(SingleThreadedBottomConstellation.class, identifier);
        
        logger.warn("Starting SingleThreadedBottomCohort: " + identifier);
        
        String tmp = p.getProperty("ibis.cohort.steal.delay");
        
        if (tmp != null && tmp.length() > 0) { 
            stealDelay = Integer.parseInt(tmp);
        } else { 
            stealDelay = DEFAULT_STEAL_DELAY;
        }
        
        logger.warn("SingleThreaded: steal delay set to " + stealDelay + " ms.");
        
        /*
        String tmp = p.getProperty("ibis.cohort.sleep");
        
        if (tmp != null && tmp.length() > 0) { 
            sleepTime = Integer.parseInt(tmp);
        } else { 
            sleepTime = 1000;
        }
        
        logger.warn("SingleThreaded: sleepTime set to " + sleepTime + " ms.");
        */
        
        tmp = p.getProperty("ibis.cohort.steal.size");
        
        if (tmp != null && tmp.length() > 0) { 
            stealSize = Integer.parseInt(tmp);
        } else { 
            stealSize = 1;
        }
        
        logger.warn("SingleThreaded: steal size set to " + stealSize);
        
        tmp = p.getProperty("ibis.cohort.steal.ignorereplies");
        
        if (tmp != null && tmp.length() > 0) { 
            ignoreEmptyStealReplies = Boolean.parseBoolean(tmp);
        } else { 
            ignoreEmptyStealReplies = DEFAULT_IGNORE_EMPTY_STEAL_REPLIES;
        }
        
        logger.warn("SingleThreaded: ignore empty steal replies set to " 
                + ignoreEmptyStealReplies);
        
        if (PROFILE) {/*
            profileTime = System.currentTimeMillis();
            profileDeadline = profileTime + profileDelta;
            
            management = ManagementFactory.getThreadMXBean();

            if (management.isThreadCpuTimeSupported()
                    && !management.isThreadCpuTimeEnabled()) {
                management.setThreadCpuTimeEnabled(true);
            }

            if (management.isThreadContentionMonitoringSupported()
                    && !management.isThreadContentionMonitoringEnabled()) {
                management.setThreadContentionMonitoringEnabled(true);
            }*/
        }

        sequential = new ExecutorWrapper(this, executor, p, identifier, logger);
   
        myPool = sequential.belongsTo();
        stealPool = sequential.stealsFrom();
        
        parent.register(this);
    }
    
    public StealPool belongsTo() { 
    	return myPool;
    }
    
    public StealPool stealsFrom() { 
    	return stealPool;
    }
    
    
    
    
  /*  
    private void warning(String message) { 
        System.err.println(message);
        new Exception().printStackTrace(System.err);
    }
    */

    /* ===================== BottomCohort Interface ==========================*/
    
    public void setContext(ConstellationIdentifier id, WorkerContext c) throws Exception { 

     //   System.out.println("Setting context of " + id + " to " + c);
        
        if (!identifier.equals(id)) { 
            throw new Exception("Received stray contextChange! " + c);
        } 
       
        postContextChange(c);
    }

    public WorkerContext getContext() { 
        // NOTE: this context may lag behind the value provided in setContext!
        return sequential.getContext();
    }
    
    public ConstellationIdentifier identifier() { 
        return identifier;
    }
    
    public boolean activate() { 
       
        //TODO: protect with var+lock! 
        start();
        return true;
    }

    public synchronized void done() {
        done = true;
    }
    
    public boolean canProcessActivities() { 
        return true;
    }
    
    public ActivityIdentifier deliverSubmit(Activity a) { 
        return submit(a);
    }
    
    public void deliverStealRequest(StealRequest sr) { 
        
        logger.info("S REMOTE STEAL REQUEST from " + sr.source 
                + " context " + sr.context);
        
        postStealRequest(sr);
    }
    
    public void deliverLookupRequest(LookupRequest lr) {
        postLookupRequest(lr);
    }
    
    public void deliverStealReply(StealReply sr) {
  
      //  System.err.println("ST STEAL reply: " + Arrays.toString(sr.getWork()));
        
        if (!sr.isEmpty()) { 
            postActivityRecord(sr.getWork());
        }
    }
        
    public void deliverEventMessage(ApplicationMessage m) {
        postApplicationMessage(m);
    }
    
    public void deliverUndeliverableEvent(UndeliverableEvent ue) {
        logger.warning("Got UndeliverableEvent from: " + ue.source);
    }
    
    public void deliverLookupReply(LookupReply lr) {
        logger.warning("Got unexpected LookupReply from: " + lr.source);
    }

    /*============= Needed by sequential sub cohort ==========================*/
    
    protected ActivityIdentifierFactory getActivityIdentifierFactory(
            ConstellationIdentifier cid) { 
        return parent.getActivityIdentifierFactory(identifier);
    }
    
    protected void contextChanged(WorkerContext c) { 
        logger.fixme("UNIMPLEMENTED contextChanged");
    } 
        
    protected boolean isMaster() {
        return false;
    }
  
    protected boolean deregister(String name, ActivityContext scope) {
        // TODO Auto-generated method stub
        logger.fixme("UNIMPLEMENTED deregister");
        return false;
    }

    protected ActivityIdentifier lookup(String name, ActivityContext scope) {
        // TODO Auto-generated method stub
        logger.fixme("UNIMPLEMENTED lookup");
        return null;
    }

    protected boolean register(String name, ActivityIdentifier id, ActivityContext scope) {
        // TODO Auto-generated method stub
        logger.fixme("UNIMPLEMENTED register");
        return false;
    }
    
    protected void forwardEvent(Event e) {
        parent.handleApplicationMessage(new ApplicationMessage(sequential.identifier(), e));
    }

    
/*
    public Cohort[] getSubCohorts() {
        return new Cohort [] { sequential };
    }
  */  
    /*
    public PrintStream getOutput() {
        return out;
    }
    */
    
    /*
    private void cancel(ActivityIdentifier id) {
        synchronized (incoming) { 
            incoming.pendingCancelations.add(id);
        }
        havePendingRequests = true;
    }*/
    
    private final void signal() { 
        havePendingRequests = true;
        //thread.interrupt();
    }
    
    private void postStealRequest(StealRequest s) {
        synchronized (incoming) {
            
            if (Debug.DEBUG_STEAL) { 
                StealRequest tmp = incoming.stealRequests.get(s.source);
            
                if (tmp != null) { 
                    logger.warn("Steal request overtaken: " + s.source);
                    //System.out.println("Steal request overtaken: " + s.source);
                }
            }
       
            incoming.stealRequests.put(s.source, s);
        }
        //havePendingRequests = true;
        signal();
    }
    
    private void postContextChange(WorkerContext c) { 
        synchronized (incoming) { 
            incoming.newContext = c;
        }
        //havePendingRequests = true;
        signal();
    }
    
    private void postLookupRequest(LookupRequest s) {
        
        System.out.println("Received lookup request for " + s.missing 
                + " from " + s.source); 
        
        synchronized (incoming) { 

            LookupRequest tmp = incoming.lookupRequests.get(s.source);
            
            if (tmp != null) { 
            	if (Debug.DEBUG_LOOKUP) { 
            		logger.warn("FIXME: overriding lookup request! Source " 
            				+ s.source + " lookup for " + s.missing);
            	}
            }
            
            incoming.lookupRequests.put(s.source, s);
        }
        //havePendingRequests = true;
        signal();
    }
    
    private void postApplicationMessage(ApplicationMessage m) {
        synchronized (incoming) { 
            incoming.deliveredApplicationMessages.add(m);
        }
        //havePendingRequests = true;
        signal();
    }
    
    private void postActivityRecord(ActivityRecord [] a) {
        
        if (a == null || a.length == 0) { 
            return;
        }
        
        for (int i=0;i<a.length;i++) { 
            if (a[i] != null) { 
                postActivityRecord(a[i]);
            }
        }
    }
        
    private void postActivityRecord(ActivityRecord a) {
        
        synchronized (incoming) { 
            incoming.deliveredActivityRecords.add(a);
        }
        // havePendingRequests = true;
        signal();
    }
    
    private ActivityIdentifier submit(Activity a) {

    //    System.out.println("ST SUBMIT activity with context " + a.getContext());
        
        ActivityIdentifier id = sequential.prepareSubmission(a);
        
        if (Debug.DEBUG_SUBMIT) { 
            logger.info("submit activity " + id);
        }
        
        synchronized (incoming) {
            incoming.pendingSubmit.add(a);
        }
        
        // havePendingRequests = true;
        signal();
        return id;
    }

    /*
    private void send(Event e) {
        
        synchronized (incoming) {
            incoming.pendingEvents.add(e);
        }
        havePendingRequests = true;
    }*/

    private synchronized boolean getDone() {
        return done;
    }
  

    private void swapEventQueues() {
        
        if (Debug.DEBUG_SUBMIT && idle) { 
            logger.info("Processing events while idle!\n" + incoming.print() 
                    + "\n" + processing.print());
        }
        
        synchronized (incoming) { 
            PendingRequests tmp = incoming;
            incoming = processing;
            processing = tmp;
            // NOTE: havePendingRequests needs to be set here to prevent a gap 
            // between doing the swap + setting it to false. Another submit 
            // could potentially use this gap to insert a new event. This would 
            // lead to a race condition!
            havePendingRequests = false;
        }
    }

    private void processContextChange() { 
        if (processing.newContext != null) { 
            sequential.setContext(processing.newContext);
            processing.newContext = null;
        } 
    }

    
    private void processActivityRecords() { 
        if (processing.deliveredActivityRecords.size() > 0) {

            for (int i = 0; i < processing.deliveredActivityRecords.size(); i++) {
                sequential.addActivityRecord(processing.deliveredActivityRecords.get(i));
            }

            processing.deliveredActivityRecords.clear();
        }
    }
    
    private void processSubmits() { 
        
        if (processing.pendingSubmit.size() > 0) {
         
            if (Debug.DEBUG_SUBMIT) { 
                logger.info("processing " + processing.pendingSubmit.size() 
                        + " submits");
            }
            
            for (int i = 0; i < processing.pendingSubmit.size(); i++) {
                sequential.finishSubmission(processing.pendingSubmit.get(i));
            }

            processing.pendingSubmit.clear();
        }
    }
    
    /*
    
    private void processLocalEvents() { 
        if (processing.pendingEvents.size() > 0) {

            for (int i = 0; i < processing.pendingEvents.size(); i++) {

                Event e = processing.pendingEvents.get(i);

                if (!sequential.queueEvent(e)) {
                    // Failed to deliver event locally, so dispatch to parent
                    
                    ApplicationMessage m = new ApplicationMessage(
                            sequential.identifier(), e);
                    parent.handleMessage(m);
                }
            }

            processing.pendingEvents.clear();
        }
    }
    */
    
    private void processRemoteMessages() { 
        if (processing.deliveredApplicationMessages.size() > 0) {

            for (int i = 0; i < processing.deliveredApplicationMessages.size(); i++) {

                ApplicationMessage m = processing.deliveredApplicationMessages.get(i);

                if (!sequential.queueEvent(m.event)) {
                    // Failed to deliver event locally, so dispatch to parent
                  
                    logger.warning("Failed to deliver message from " 
                            + m.source + " / " + m.event.source + " to " 
                            + m.target + " / " + m.event.target + " payload " 
                            + m.event);
                    
                    UndeliverableEvent u = new UndeliverableEvent(
                            sequential.identifier(), m.source, m.event);
                    
                    parent.handleUndeliverableEvent(u);
                    
                    //System.exit(1);
                }
            }

            processing.deliveredApplicationMessages.clear();
        }
    }
    
    private void processCancellations() { 
        if (processing.pendingCancelations.size() > 0) {

            for (int i = 0; i < processing.pendingCancelations.size(); i++) {
                sequential.cancel(processing.pendingCancelations.get(i));
            }

            processing.pendingCancelations.clear();
        }
    }

    private void processStealRequests() { 
        
      //  System.out.println("Processing steal requests " + processing.stealRequests.size());
        
        if (processing.stealRequests.size() == 0) { 
            return;
        }
        
        for (StealRequest s : processing.stealRequests.values()) { 
        
            // Make sure the steal request is still valid!
            if (!s.getStale()) { 
                
                // NOTE: a is allowed to be null
                ActivityRecord [] a = sequential.steal(s.context, s.allowRestricted(), stealSize);
               
                if (a != null || !ignoreEmptyStealReplies) { 
                
                   // logger.warn("SENDING STEAL REPLY: " + s.source + " /" + s.context + " " + (a==null ? "0" : a.length));
                    
                    // We either have a result, or we always send a reply
                    parent.handleStealReply(
                            new StealReply(sequential.identifier(), 
                            s.source, a));              
                } else { 
                    // No result, and we're not supposed to tell anyone  
                    if (Debug.DEBUG_STEAL) { 
                    	logger.info("IGNORING empty steal reply");
                    } 
                }
                    
                //    if (!parent.forwardStealReply(s, a)) { 
                //        // The parent was no longer interested in the steal reply, 
                //        // so just return the job to the queue
                //        sequential.addActivityRecord(a);
                //    }              
            } else { 
                
                System.out.println("DROPPING STALE STEAL REQUEST");
                
            }
        }
        
        processing.stealRequests.clear();
    }

    private void processLookupRequests() { 
    
        if (processing.lookupRequests.size() == 0) { 
            return;
        }
        
        for (LookupRequest s : processing.lookupRequests.values()) { 
        
            // Make sure the steal request is still valid!
            if (!s.getStale()) { 
                ActivityRecord a = sequential.lookup(s.missing);
                
                if (a != null) { 
                
                    if (Debug.DEBUG_LOOKUP) { 
                        logger.info("Sending lookup reply for " + s.missing 
                                + " to " + s.source);
                    }
                
                    System.out.println("(FOUND) Sending lookup reply for " + s.missing 
                            + " to " + s.source);
                    
                    LookupReply tmp = new LookupReply(sequential.identifier(), 
                            s.source, s.missing, sequential.identifier(), 
                            a.getHopCount());
                    parent.handleLookupReply(tmp);
                }
            } else { 
                System.out.println("DROPPING STALE LOOKUP!");
            
            }
        }
    }
    
    private void processEvents() {
    
        // TODO: think about the order here ?
        swapEventQueues();
        
   //     System.out.println("Events waiting: " + 
   //             (processing.newContext != null) + " " + 
   //             processing.deliveredActivityRecords + " " + 
   //             processing.deliveredApplicationMessages + " " + 
   //             processing.lookupRequests + " " +
   //             processing.pendingCancelations + " " +
   //             processing.pendingSubmit + " " +
   //             processing.stealRequests);
        
        processContextChange();
        
        processActivityRecords();
        processSubmits();
      //  processLocalEvents();
        processRemoteMessages();
        processCancellations();
        processStealRequests();
        processLookupRequests();
    }
    
    /*
    private boolean pause(long time) { 
        
        if (time > 0) { 

            long end = System.currentTimeMillis() + time;

            boolean wake = havePendingRequests || getDone(); 

            while (!wake) { 

                try { 
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    // ignored
                }

                wake = havePendingRequests 
                    || (System.currentTimeMillis() > end) 
                    || getDone();  
            }
        }
        
        return (havePendingRequests || getDone());
    }
    */

    private boolean pauseUntil(long deadline) { 
        
        long pauseTime = deadline - System.currentTimeMillis();
        
        if (deadline > 0) { 

            boolean wake = havePendingRequests || getDone(); 

            while (!wake) { 
                
            //    String tmp = sequential.printState();
                
           //     logger.warn("Cohort sleeping(" + pauseTime +")");
                
                /* EEP: print this caused an occasional ConcurrentMod.Exception 
                       because the ArrayLists in the datastructs may be changed 
                       while this print is active...
             
                       We could synchronize it, but as this is just a debug 
                       print this is pretty useless...  
                        
                with state: " 
                        + tmp 
                        + "\n and processing events: " + 
                             (processing.newContext != null) + " " + 
                             processing.deliveredActivityRecords + " " + 
                             processing.deliveredApplicationMessages + " " + 
                             processing.lookupRequests + " " +
                             processing.pendingCancelations + " " +
                             processing.pendingSubmit + " " +
                             processing.stealRequests 
                        + "\n and incoming events: " + 
                             (incoming.newContext != null) + " " + 
                             incoming.deliveredActivityRecords + " " + 
                             incoming.deliveredApplicationMessages + " " + 
                             incoming.lookupRequests + " " +
                             incoming.pendingCancelations + " " +
                             incoming.pendingSubmit + " " +
                             incoming.stealRequests); 
             */
                
                //interrupted(); // Clear flag
                //LockSupport.parkNanos(pauseTime * 1000);
                
                try { 
                    Thread.sleep(pauseTime);
                } catch (Exception e) {
                    // ignored
                }
                
                
                wake = havePendingRequests || getDone()
                    || (System.currentTimeMillis() > deadline); 
            
                if (!wake) { 
                    pauseTime = deadline - System.currentTimeMillis();
                    wake = (pauseTime <= 0); 
                }
            }
        }
        
        return (havePendingRequests || getDone());
    }
    
    private long stealAllowed() { 
  
       if (THROTTLE_STEALS) { 
           
           long now = System.currentTimeMillis();
           
           if (now >= nextStealDeadline) { 
               nextStealDeadline = now + stealDelay;
               return 0;
           }
           
           return nextStealDeadline;
       }
       
       return 0;
    }

    private CircularBuffer wrongContext = new CircularBuffer(1);
    
    protected void push(ActivityRecord a) { 
        wrongContext.insertLast(a);
    }
    
    protected void processActivity() { 
    	
    }

    private long start;
    private long idlestart;
    
    protected boolean processActivities() { 

    	long t1 = System.currentTimeMillis();
        	
    	if (havePendingRequests) { 
    		processEvents();
    	}

    	while (wrongContext.size() > 0) { 
    		parent.handleWrongContext(
    				(ActivityRecord) wrongContext.removeLast());
    	}
            
    	long t2 = System.currentTimeMillis();
            
    	// NOTE: one problem here is that we cannot tell if we did any work 
    	// or not. We would like to know, since this allows us to reset
    	// several variables (e.g., sleepIndex)

    	int jobs = 0;

    	boolean more = sequential.process();

    	if (more) {
    		jobs++;
    	}

    	while (more && !havePendingRequests) {
    		more = sequential.process();

    		if (more) {
    			jobs++;
    		}
    	}

    	long t3 = System.currentTimeMillis();

    	if (jobs > 0) { 
    		if (t2-idlestart > 0) { 
    			out.println("IDLE from " + (idlestart-start) + " to " + (t2-start) + " total " + (t2-idlestart));
    		}

    		idlestart = t3;

    		out.println("ACTIVE from " + (t2-start) + " to " 
    				+ (t3-start) + " total " + (t3-t2) + " jobs " + jobs);

    		out.flush();
    	}

    	while (!more && !havePendingRequests) {

    		long nextDeadline = stealAllowed();

    		if (nextDeadline == 0) { 

    			if (Debug.DEBUG_STEAL) { 
    				logger.info("GENERATING STEAL REQUEST at " + identifier + " with context " + getContext());
    			} 

    			StealRequest sr = new StealRequest(identifier, getContext(), stealPool);
    			ActivityRecord ar = parent.handleStealRequest(sr);

    			if (ar != null) { 
    				sequential.addActivityRecord(ar);
    				more = true;
    			}
    		} else { 
    			more = pauseUntil(nextDeadline);
    		}
    	}

    	long t4 = System.currentTimeMillis();

    	eventTime   += t2 - t1;
    	activeTime  += t3 - t2;
    	idleTime    += t4 - t3;
    	
    	return getDone();
    }
        
    public void run() { 
    	
    	start = System.currentTimeMillis();
    	idlestart = start;
    
    	sequential.runExecutor();
    	
    	long time = System.currentTimeMillis() - start;

        printStatistics(time);
    }
    
    
    
    /*
    public void run() {

        // NOTE: For D&C applications it seems to be most efficient to
        // process a single command (i.e., a submit or an event) and then
        // process all changes that occurred in the activities.
    	
        //WorkQueue wrongContext = sequential.getWrongContextQueue();
        
        long start = System.currentTimeMillis();
        long idlestart = start;

        while (!getDone()) {

            long t1 = System.currentTimeMillis();

        //    if (PROFILE && t1 > profileDeadline) {
       //         printProfileInfo(t1);
        //        profileDeadline = t1 + profileDelta;
        //    }

            if (havePendingRequests) { 
                processEvents();
            }

            while (wrongContext.size() > 0) { 
                parent.handleWrongContext(
                        (ActivityRecord) wrongContext.removeLast());
            }
            
            long t2 = System.currentTimeMillis();
            
            // NOTE: one problem here is that we cannot tell if we did any work 
            // or not. We would like to know, since this allows us to reset
            // several variables (e.g., sleepIndex)
         
            int jobs = 0;
            
            boolean more = sequential.process();

            if (more) {
                jobs++;
            }
            
            while (more && !havePendingRequests) {
                more = sequential.process();
            
                if (more) {
                    jobs++;
                }
            }
                   
            long t3 = System.currentTimeMillis();
            
            if (jobs > 0) { 

                if (t2-idlestart > 0) { 
                    out.println("IDLE from " + (idlestart-start) + " to " + (t2-start) + " total " + (t2-idlestart));
                }
                
                idlestart = t3;
                
                out.println("ACTIVE from " + (t2-start) + " to " 
                        + (t3-start) + " total " + (t3-t2) + " jobs " + jobs);
            
                out.flush();
            }
            
            while (!more && !havePendingRequests) {
         
                // long ts = System.currentTimeMillis();
                // logger.info("IDLE");                             
               
                long nextDeadline = stealAllowed();
                
                if (nextDeadline == 0) { 

                    if (Debug.DEBUG_STEAL) { 
                        logger.info("GENERATING STEAL REQUEST at " + identifier + " with context " + getContext());
                    } 
                    
                 //   System.out.println("ST STEAL");
               
                    StealRequest sr = new StealRequest(identifier, getContext());
                    ActivityRecord ar = parent.handleStealRequest(sr);
                
                    if (ar != null) { 
                    //    more = pause(sleepTime);
                    //} else { 
                        sequential.addActivityRecord(ar);
                        more = true;
                    }
                } else { 
                    //more = pause(sleepTime);
                    more = pauseUntil(nextDeadline);
                }
                
                // logger.info("ACTIVE");                                      
                // long te = System.currentTimeMillis();               
                // out.println("IDLE from " + (ts-start) + " to " + (te-start) + " total " + (te-ts));
            }
            
            long t4 = System.currentTimeMillis();
            
            eventTime   += t2 - t1;
            activeTime  += t3 - t2;
            idleTime    += t4 - t3;
        }

        long time = System.currentTimeMillis() - start;

        printStatistics(time);
    }
    */

//    private ActivityRecord idle() { 
//        
//        ActivityRecord tmp = null;
//                
//        if (currentSteal == null || t3 > currentSteal.getTimeout()) { 
//                    
//            // Last steal is answered or has timed out
//            if (currentSteal != null) { 
//                stealTimeout++;
//            }
//                    
//                    currentSteal = new StealRequest(identifier, getContext());
//                    currentSteal.setLocal(true);
//                    currentSteal.setTimeout(t3 + 1000); // FIXME (use property)
//                        
//                    tmp = parent.stealAttempt(workerID, currentSteal);
//
//                    stealCount++;
//                } else { 
//                    tmp = parent.getStoredActivity(getContext());
//                }
//            
//                long t4 = System.currentTimeMillis();
//
//                stealTime += t4 - t3;
//                
//                if (tmp != null) { 
//                    stealSuccess++;
//                    
//                    currentSteal = null;
//                    sleepIndex= 0;
//                    sequential.addActivityRecord(tmp);
//                } else { 
//                    try {
//                        
//                        long sleepTime = SLEEP_TIMES[sleepIndex];
//                        
//                        if (sleepIndex < SLEEP_TIMES.length-1) { 
//                            sleepIndex++;
//                        }
//                        
//                        parent.workerIdle(workerID, sleepTime);
//                   
//                        Thread.sleep(sleepTime);
//                        sleepCount++;
//                    } catch (Exception e) {
//                        // ignored
//                    }
//
//                    parent.workerActive(workerID);
//                    
//                    
//                    long t5 = System.currentTimeMillis();
//
//                    sleepTime += t5 - t4;
//                }
//            }

        
   

    private void printProfileInfo(long t) {
/*
        long tempTime = t - profileTime;  
        
        long tempComputation = sequential.getComputationTime(); 
        
        long tempSubmit = sequential.getActivitiesSubmitted();
        long tempInvoke = sequential.getActivitiesInvoked();
        long tempMessageI = sequential.getMessagesInternal();
        long tempMessageE = sequential.getMessagesExternal();
        long tempSteals = sequential.getSteals();
        
        long tempCPU =  management.getCurrentThreadCpuTime();
        long tempUser = management.getCurrentThreadUserTime();
        
        ThreadInfo info = management.getThreadInfo(Thread.currentThread()
                .getId());

        long tempBlockedC = info.getBlockedCount();
        long tempBlockedT = info.getBlockedTime();

        long tempWaitC = info.getWaitedCount();
        long tempWaitT = info.getWaitedTime();
        
        StringBuilder tmp = new StringBuilder("#### ");
        tmp.append(identifier).append(" T ");
        tmp.append(t).append(" dT ");
        tmp.append(tempTime).append(" compT ");        
        tmp.append(tempComputation - profileComputation).append(" cpuT ");
        tmp.append(tempCPU - profileCPU).append(" userT ");
        tmp.append(tempUser - profileUser).append(" b# ");
        tmp.append(tempBlockedC - profileBlockedC).append(" bT ");
        tmp.append(tempBlockedT - profileBlockedT).append(" w# ");
        tmp.append(tempWaitC - profileWaitC).append(" wT ");
        tmp.append(tempWaitT - profileWaitT).append(" submit# ");
        tmp.append(tempSubmit - profileSubmit).append(" invoke# ");
        tmp.append(tempInvoke - profileInvoke).append(" steal# ");
        tmp.append(tempSteals - profileSteals).append(" messI# ");
        tmp.append(tempMessageI - profileMessageI).append(" messE# ");
        tmp.append(tempMessageE - profileMessageE).append(" ");
        
        synchronized (System.err) {
            System.err.println(tmp.toString());
        }
     
        profileTime = t;
        profileComputation = tempComputation;
        profileCPU = tempCPU;
        profileUser = tempUser;
        profileBlockedC = tempBlockedC;
        profileBlockedT = tempBlockedT;
        profileWaitC = tempWaitC;
        profileWaitT = tempWaitT;
        profileSubmit = tempSubmit;
        profileInvoke = tempInvoke;
        profileMessageI = tempMessageI;
        profileMessageE = tempMessageE;
        profileSteals = tempSteals;        
        */
    }

    public void printStatistics(long totalTime) {

        printProfileInfo(System.currentTimeMillis());
        
        long cpuTime = 0;
        long userTime = 0;
        double cpuPerc = 0.0;
        double userPerc = 0.0;

        long blocked = 0;
        long blockedTime = 0;

        long waited = 0;
        long waitedTime = 0;

        double blockedPerc = 0.0;
        double waitedPerc = 0.0;

        final long messagesInternal = sequential.getMessagesInternal();
        final long messagesExternal = sequential.getMessagesExternal();
        final long messagesTime = sequential.getMessagesTime();

        final long computationTime = sequential.getComputationTime() - messagesTime;
        final long activitiesInvoked = sequential.getActivitiesInvoked();
   
        final long activitiesSubmitted = sequential.getActivitiesSubmitted();
        final long activitiesAdded = sequential.getActivitiesSubmitted();
        
        final long wrongContextSubmitted = sequential.getWrongContextSubmitted();
        final long wrongContextAdded = sequential.getWrongContextAdded();
        final long wrongContextDiscovered = sequential.getWrongContextDicovered();
             
        final long steals = sequential.getSteals();
        final long stealSuccessIn = sequential.getStealSuccess();
        final long stolen = sequential.getStolen();
        
        final double comp = (100.0 * computationTime) / totalTime;
        final double fact = ((double) activitiesInvoked) / activitiesSubmitted;

        final double eventPerc = (100.0 * eventTime) / totalTime;
        final double activePerc = (100.0 * activeTime) / totalTime;
        final double idlePerc = (100.0 * idleTime) / totalTime;
        final double messPerc = (100.0 * messagesTime) / totalTime;
        
        if (PROFILE) {
            // Get the cpu/user time (in nanos)
            /*
            cpuTime = management.getCurrentThreadCpuTime();
            userTime = management.getCurrentThreadUserTime();

            cpuPerc = (cpuTime / 10000.0) / totalTime;
            userPerc = (userTime / 10000.0) / totalTime;

            cpuTime = cpuTime / 1000000L;
            userTime = userTime / 1000000L;

            ThreadInfo info = management.getThreadInfo(Thread.currentThread()
                    .getId());

            blocked = info.getBlockedCount();
            blockedTime = info.getBlockedTime();

            waited = info.getWaitedCount();
            waitedTime = info.getWaitedTime();

            blockedPerc = (100.0 * blockedTime) / totalTime;
            waitedPerc = (100.0 * waitedTime) / totalTime;
            */
        }

        synchronized (out) {

            out.println(identifier + " statistics");
            out.println(" Time");
            out.println("   total      : " + totalTime + " ms.");
            out.println("   active     : " + activeTime + " ms. ("
                    + activePerc + " %)");
            out.println("        run() : " + computationTime + " ms. ("
                    + comp + " %)");

            out.println("   command    : " + eventTime + " ms. ("
                    + eventPerc + " %)");

            out.println("   idle count: " + idleCount);
            out.println("   idle time : " + idleTime + " ms. ("
                    + idlePerc + " %)");

            out.println("   mess time : " + messagesTime + " ms. ("
                    + messPerc + " %)");
            
            if (PROFILE) {

                out.println("   cpu time   : " + cpuTime + " ms. ("
                        + cpuPerc + " %)");

                out.println("   user time  : " + userTime + " ms. ("
                        + userPerc + " %)");

                out.println("   blocked    : " + blocked + " times");

                out.println("   block time : " + blockedTime + " ms. ("
                        + blockedPerc + " %)");

                out.println("   waited     : " + waited + " times");

                out.println("   wait time  : " + waitedTime + " ms. ("
                        + waitedPerc + " %)");

            }

            out.println(" Activities");
            out.println("   submitted  : " + activitiesSubmitted);
            out.println("   added      : " + activitiesAdded);
            out.println("   invoked    : " + activitiesInvoked + " ("
                    + fact + " /act)");
            out.println("  Wrong Context");
            out.println("   submitted  : " + wrongContextSubmitted);
            out.println("   added      : " + wrongContextAdded);
            out.println("   discovered : " + wrongContextDiscovered);
            out.println(" Messages");
            out.println("   internal   : " + messagesInternal);
            out.println("   external   : " + messagesExternal);
            out.println(" Steals");
            out.println("   incoming   : " + steals);
            out.println("   success    : " + stealSuccessIn);
            out.println("   stolen     : " + stolen);
        }
        
        out.flush();        
    }

    public ConstellationIdentifier [] getLeafIDs() { 
        return new ConstellationIdentifier [] { identifier };
    }   
     
    public void setContext(WorkerContext context) {
        
      //  System.out.println("Setting context of ST to " + context);
        
        sequential.setContext(context);
    }

    public void clearContext() {
        sequential.clearContext();
    }

	public void addToLookupCache(ActivityIdentifier aid) {
		parent.addToLookupCache(identifier, aid);
	}
    
   

    
    /* This is the part subcohort interface that is not shared with the 
     * Cohort interface */
    
    /*

    public void deliverCancel(ActivityIdentifier aid) {
        cancel(aid);
    }

    public void deliverLookup(LookupRequest lr) {
        postLookupRequest(lr);
    }

    public void deliverSteal(StealRequest sr) {
        postStealRequest(sr);
    }

    public void deliverStealReply(StealReply sr) {
        if (sr.work != null) { 
            deliverActivityRecord(sr.work);
        }
    }

    public ActivityIdentifier deliverSubmit(Activity a) {
        return submit(a);
    }
   
    public void deliverMessage(Message m) {
        // TODO Auto-generated method stub
        
    }
   */ 
    
    /*
	public void registerPool(Executor executor, StealPool myPool,
			boolean poolIsFixed) {
	
		if (this.poolIsFixed) { 
			throw new IllegalStateException("Pool already set and fixed!");
		}
		
		StealPool old = myPool;
		
		this.myPool = myPool;
		this.poolIsFixed = poolIsFixed;

		if (parent != null) { 
			parent.registerPool(this, old, myPool);
		}
	}

	public void registerStealPool(Executor executor, StealPool stealsFrom,
			boolean stealIsFixed) {

		if (this.stealIsFixed) { 
			throw new IllegalStateException("Pool already set and fixed!");
		}
		
		StealPool old = stealsFrom;
		
		this.stealPool = stealsFrom;
		this.stealIsFixed = stealIsFixed;

		if (parent != null) { 
			parent.registerStealPool(this, old, stealsFrom);
		}
	}
     */
    
}




