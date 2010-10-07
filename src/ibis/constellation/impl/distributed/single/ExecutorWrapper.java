package ibis.constellation.impl.distributed.single;

import ibis.constellation.Activity;
import ibis.constellation.ActivityContext;
import ibis.constellation.ActivityIdentifier;
import ibis.constellation.ActivityIdentifierFactory;
import ibis.constellation.Cohort;
import ibis.constellation.CohortIdentifier;
import ibis.constellation.Event;
import ibis.constellation.Executor;
import ibis.constellation.MessageEvent;
import ibis.constellation.StealPool;
import ibis.constellation.WorkerContext;
import ibis.constellation.context.UnitWorkerContext;
import ibis.constellation.extra.CircularBuffer;
import ibis.constellation.extra.CohortLogger;
import ibis.constellation.extra.Debug;
import ibis.constellation.extra.SmartSortedWorkQueue;
import ibis.constellation.extra.WorkQueue;
import ibis.constellation.impl.distributed.ActivityRecord;

import java.util.HashMap;
import java.util.Properties;

public class ExecutorWrapper implements Cohort {

    private static final boolean PROFILE = true;

	private static int QUEUED_JOB_LIMIT = 1;

    private final SingleThreadedBottomCohort parent;

    private final CohortIdentifier identifier;

    // private PrintStream out;
    private final CohortLogger logger;

    private final Executor executor;
    
    // Default context is DEFAULT
    private WorkerContext myContext = UnitWorkerContext.DEFAULT; 
    private boolean contextChange = false;
    
    private HashMap<String, ActivityIdentifier> registry = 
        new HashMap<String, ActivityIdentifier>();

    private HashMap<ActivityIdentifier, ActivityRecord> lookup = 
        new HashMap<ActivityIdentifier, ActivityRecord>();
       
    private WorkQueue restricted;
    private WorkQueue fresh;
    
    private CircularBuffer runnable = new CircularBuffer(1);
    private CircularBuffer stolen = new CircularBuffer(1);

    private ActivityIdentifierFactory generator;
    
    private long computationTime;

    private long activitiesSubmitted;
    private long activitiesAdded;

    private long wrongContextSubmitted;
    private long wrongContextAdded;

    private long wrongContextDicovered;

    private long activitiesInvoked;

    private long steals;
    private long stealSuccess;
    private long stolenJobs;

    private long messagesInternal;
    private long messagesExternal;
    private long messagesTime;

    private ActivityRecord current;

    ExecutorWrapper(SingleThreadedBottomCohort parent, Executor executor, Properties p, 
            CohortIdentifier identifier, CohortLogger logger) throws Exception {
        this.parent = parent;
        this.identifier = identifier;
        this.generator = parent.getActivityIdentifierFactory(identifier);
        this.logger = logger;
        this.executor = executor;

        QUEUED_JOB_LIMIT = Integer.parseInt(p.getProperty("ibis.cohort.queue.limit", "100000"));

        System.out.println("Executor set job limit to " + QUEUED_JOB_LIMIT);
        
        restricted = new SmartSortedWorkQueue("Br(" + identifier + ")");
        fresh = new SmartSortedWorkQueue("Bf(" + identifier + ")");
         
        executor.connect(this);
        
        myContext = executor.getContext();
    }

    /*
    public BaseCohort(Properties p, WorkerContext context) {
        this.parent = null;

        if (context == null) { 
            myContext = UnitWorkerContext.DEFAULT;
        } else { 
            myContext = context;
        }

        this.identifier = new CohortIdentifier(0);
        this.generator = new ActivityIdentifierFactory(0, 0, Long.MAX_VALUE);
        this.logger = CohortLogger.getLogger(BaseCohort.class, identifier);
    }        
     */
    
    public void cancel(ActivityIdentifier id) {

        ActivityRecord ar = lookup.remove(id);

        if (ar == null) {
            return;
        }

        if (ar.needsToRun()) {
            runnable.remove(ar);
        }
    }

    public void done() {
        if (lookup.size() > 0) {        
            logger.warning("Quiting Cohort with " + lookup.size()
                    + " activities in queue");
        }
    }

  //  protected WorkQueue getWrongContextQueue() { 
  //      return wrongContext;
  //  }

    private void cleanQueues() {
    	System.err.println("TODO: implement cleanQueues() after context change!");
    	logger.error("TODO: implement cleanQueues() after context change !");
    	contextChange = false;
    }
    
    private ActivityRecord dequeue() {

    	if (contextChange) { 
    		cleanQueues();
    	}

    	int size = stolen.size();

        if (size > 0) {
            return (ActivityRecord) stolen.removeFirst();
        }
    	
        size = runnable.size();

        if (size > 0) {
            return (ActivityRecord) runnable.removeFirst();
        }

        size = restricted.size();
        
        // FIXME: These used to dequeue the tail, now they dequeue the head...
        
        if (size > 0) { 
            return restricted.dequeue(true);
        }
        
        size = fresh.size();
        
        if (size > 0) { 
            return fresh.dequeue(true);
        }
        
        return null;
    }

    private ActivityIdentifier createActivityID() {

        try {
            return generator.createActivityID();
        } catch (Exception e) {
            // Oops, we ran out of IDs. Get some more from our parent!
            if (parent != null) { 
                generator = parent.getActivityIdentifierFactory(identifier);
            }
        }

        try {
            return generator.createActivityID();
        } catch (Exception e) {
            throw new RuntimeException(
                    "INTERNAL ERROR: failed to create new ID block!", e);
        }

        // return new MTIdentifier(nextID++);
    }

    public ActivityIdentifier prepareSubmission(Activity a) {

        ActivityIdentifier id = createActivityID();
        a.initialize(id);

        if (Debug.DEBUG_SUBMIT) {
            logger.info("created " + id + " at " 
                    + System.currentTimeMillis() + " from " 
                    + (current == null ? "ROOT" : current.identifier()));
        }
        return id;
    }

    public void finishSubmission(Activity a) {

    //    System.out.println("BASE: LOCAL got work " + a.getContext());      
        
        activitiesSubmitted++;

        //  if (activitiesSubmitted % 10000 == 0) { 
        //      System.out.println("BASE(" + identifier + ") submit " + a.identifier() + " " + activitiesSubmitted);
        //  }

        ActivityRecord ar = new ActivityRecord(a);
        ActivityContext c = a.getContext();

        /*
        if (c.isLocal()) { 
            System.out.println("BASE: LOCAL Work inserted in LOCAL " + c);      

            local.insertLast(ar);
        } else*/
       
        if (restricted.size() + fresh.size() >= QUEUED_JOB_LIMIT) {
        	// If we have too much work on our hands we push it to out parent. Added bonus 
        	// is that others can access it without interrupting me.
        	
        	System.out.println("Executor pushing work to parent! " + restricted.size() + " " + fresh.size());
        	
            parent.push(ar);
            return;
        }
        
        if (c.satisfiedBy(myContext)) { 
        	
            // System.out.println("BASE(" + identifier + ") submit " + a.identifier() + " COMPLETED");

            lookup.put(a.identifier(), ar);
            
            if (ar.isRestrictedToLocal()) { 
                restricted.enqueue(ar);
           //     System.out.println("BASE: LOCAL Work inserted in RESTRICTED " + c + " " + a.identifier());      
            } else { 
                fresh.enqueue(ar);
          //      System.out.println("BASE: LOCAL Work inserted in FRESH " + c + " " + a.identifier());      
            }
  
        } else {

            System.out.println("BASE: LOCAL Work inserted in WRONG " + c + " " + a.identifier());      
            
            //logger.info("submitted " + a.identifier() + " with WRONG CONTEXT " + c);

            parent.push(ar);
            
            // wrongContextSubmitted++;
            // wrongContext.enqueue(ar);

            //wrongContext.insertLast(ar);
        }

        if (Debug.DEBUG_SUBMIT) { 
            logger.info("SUBMIT BASE(" + identifier + "): activities " 
                    + fresh.size() + " " /*+ wrongContext.size()*/ + " " + runnable.size() + " " + lookup.size());
        }

        //  synchronized (this) {
        //      System.out.println("sync");
        // } 

    }

    void addActivityRecord(ActivityRecord a) {

        if (Debug.DEBUG_SUBMIT) {
            logger.info("received " + a.identifier() + " at " 
                    + System.currentTimeMillis());
        }

        activitiesAdded++;

        ActivityContext c = a.activity.getContext();
        
        if (restricted.size() + fresh.size() >= QUEUED_JOB_LIMIT) {
        	// If we have too much work on our hands we push it to out parent. Added bonus 
        	// is that others can access it without interrupting me.
        	
        	System.out.println("* Executor pushing work to parent! " + restricted.size() + " " + fresh.size());
        	
            parent.push(a);
            return;
        }
        
        if (c.satisfiedBy(myContext)) { 
        	
            lookup.put(a.identifier(), a);

            // Always true ?
            if (a.isStolen()) { 
            	stolen.insertLast(a);
            } else if (a.isFresh()) {
                if (a.isRestrictedToLocal()) {
                    System.out.println(identifier + " BASE: got REMOTE work in RESTRICTED " + c + " " + a.identifier());      
                    restricted.enqueue(a);
                } else { 
                    System.out.println(identifier + " BASE: got REMOTE work in FRESH " + c + " " + a.identifier());      
                    fresh.enqueue(a);
                }
            } else {
                System.out.println(identifier + " BASE: got REMOTE work in RUNNABLE " + c + " " + a.identifier());      
                runnable.insertLast(a);
            }

        } else {

            System.out.println("BASE: got REMOTE work in WRONG " + c);      

          //  wrongContextAdded++;

            //wrongContext.insertLast(a);
        //    wrongContext.enqueue(a);
        
            parent.push(a);
        }
    }

    protected ActivityRecord lookup(ActivityIdentifier id) { 
        return lookup.get(id);
    }

    public ActivityIdentifier submit(Activity a) {

    	if (current != null) {
    		// since the current activity performed a submit, we assume someone will 
    		// be interested in locating it later on.
    		parent.addToLookupCache(current.activity.identifier());
    	}
    	
        ActivityIdentifier id = prepareSubmission(a);
        finishSubmission(a);
        return id;
    }

    private ActivityIdentifier lookup(String name) { 
        return registry.get(name);
    }

    private boolean register(String name, ActivityIdentifier id) {

        if (registry.containsKey(name)) { 
            return false;
        }

        registry.put(name, id);
        return true;
    }

    private boolean deregister(String name) {
        return (registry.remove(name) != null);
    }

    public ActivityIdentifier lookup(String name, ActivityContext scope) {

        // TODO: does this still make sense ?
        if (parent == null /*|| scope.isRestrictedToLocal()*/) { 
            return lookup(name);
        }

        return parent.lookup(name, scope);
    }    

    public boolean register(String name, ActivityIdentifier id, ActivityContext scope) {

        // TODO: does this still make sense ?
        if (parent == null /*|| scope.isRestrictedToLocal()*/) { 
            return register(name, id);
        }

        return parent.register(name, id, scope);
    }

    public boolean deregister(String name, ActivityContext scope) {

        // TODO: does this still make sense ?
        if (parent == null /*|| scope.isRestrictedToLocal()*/) { 
            return deregister(name);
        }

        return parent.deregister(name, scope);
    }

    public void send(ActivityIdentifier source, ActivityIdentifier target, 
            Object o) { 
        send(new MessageEvent(source, target, o));
    }

    public void send(Event e) {

        long start, end;

        if (PROFILE) { 
            start = System.currentTimeMillis();
        }

        if (Debug.DEBUG_EVENTS) {
            logger.info("SEND EVENT " + e.source + " to " 
                    + e.target + " at " + start);
        }

        ActivityRecord ar = lookup.get(e.target);

        if (ar == null) {
            // Send isn't local, so forward to parent.

            messagesExternal++;

            if (parent == null) { 
                throw new RuntimeException("UNKNOWN TARGET: failed to find " +
                        "destination activity " + e.target);
            } else { 
                parent.forwardEvent(e);
            }

        } else {

            messagesInternal++;

            ar.enqueue(e);

            boolean change = ar.setRunnable();

            if (change) {
                runnable.insertLast(ar);
            }
        }

        if (PROFILE) { 
            end = System.currentTimeMillis();
            messagesTime += (end-start);        
        }
    }

    public boolean queueEvent(Event e) {

        ActivityRecord ar = lookup.get(e.target);

        if (ar == null) {
            if (Debug.DEBUG_EVENTS) { 
                logger.info("CANNOT DELIVER EVENT Failed to find activity " + e.target);

                /*

                out.println("ERROR Failed to find activity " + e.target + " " + e.target.hashCode());
                out.println("ERROR Contains: " + lookup.containsKey(e.target));
                out.println("ERROR Available: ");

                Set<ActivityIdentifier> tmp = lookup.keySet();

                for (ActivityIdentifier a : tmp) { 
                    out.println(a + " " + (a.equals(e.target)) + " " + a.hashCode());      
                }*/
            } 

            return false;
        }

        // System.out.println(" SEND " + source + " -> " + target + " (" +
        // ar.activity + ") " + o);
        ar.enqueue(e);

        boolean change = ar.setRunnable();

        if (change) {
            runnable.insertLast(ar);
        }

        return true;
    }

    // UNUSED ?
    int available() { 
        return fresh.size();
    }

    ActivityRecord [] steal(WorkerContext context, boolean allowRestricted, int count) {

       // logger.warn("In STEAL on BASE " + context + " " + count);

        steals++;

        ActivityRecord [] result = new ActivityRecord[count];

        for (int i=0;i<count;i++) { 
            result[i] = doSteal(context, allowRestricted);

            if (result[i] == null) { 
        //        logger.warn("STEAL(" + count + ") only produced " + i + " results");

                if (i == 0) { 
                    return null;
                } else { 
                    stolenJobs += i;
                    stealSuccess++;                    
                    return result;
                }
            }
        }

      //  logger.warn("STEAL(" + count + ") only produced ALL results");

        stolenJobs += count;
        stealSuccess++;                    
        return result;
    }

    ActivityRecord steal(WorkerContext context, boolean remote) {

        steals++;

        ActivityRecord result = doSteal(context, remote);

        if (result != null) { 
            stealSuccess++;
            stolenJobs++;
        }

        return result;
    }

    /*
    private ActivityRecord doSteal(Context context) {

  //      synchronized (this) {
   //         System.out.println("sync");
   //    } 

        if (Debug.DEBUG_STEAL) { 
            logger.info("STEAL BASE(" + identifier + "): activities F: " 
                    + fresh.size() + " W: " + wrongContext.size() + " R: " 
                    + runnable.size() + " L: " + lookup.size());
        }

        int size = wrongContext.size();

        if (size > 0) {

            for (int i=0;i<size;i++) { 
                // Get the first of the jobs (this is assumed to be the 
                // largest one) and check if we are allowed to return it. 
                ActivityRecord r = (ActivityRecord) wrongContext.get(i);

                if (!r.isStolen()) { 

                    boolean steal = context.isAny();

                    if (!steal) { 
                        Context tmp = r.activity.getContext();


                        // FIXME: still confused about correctness of this!
                        steal = tmp.contains(context);
                        //System.err.println("COMPARE " + tmp + " with " + context + " -> " + steal);
                    }

                    if (steal) { 
                        wrongContext.remove(i);

                        lookup.remove(r.identifier());

                        if (Debug.DEBUG_STEAL) {
                            logger.info("STOLEN " + r.identifier());
                        }

                        r.setStolen(true);

                        return r;
                    }

                } else { 
                    // TODO: fix this!
                    logger.warning("MAJOR EEP!: stolen job not runnable on " 
                            + identifier);
                }
            } 
        }

        size = fresh.size();

        if (size > 0) {

            for (int i=0;i<size;i++) { 
                // Get the first of the new jobs (this is assumed to be the 
                // largest one) and check if we are allowed to return it. 
                ActivityRecord r = (ActivityRecord) fresh.get(i);

                if (!r.isStolen()) { 

                    Context tmp = r.activity.getContext();

                    if (!tmp.isLocal()) { 

                        if (context.isAny() || tmp.contains(context)) { 

                            fresh.remove(i);

                            lookup.remove(r.identifier());

                            if (Debug.DEBUG_STEAL) {
                                logger.info("STOLEN " + r.identifier());
                            }

                            r.setStolen(true);

                            return r;
                        }
                    }
                }
            } 
        }

        return null;
    }
     */

    private ActivityRecord doSteal(WorkerContext context, boolean allowRestricted) {

        if (Debug.DEBUG_STEAL) { 
            logger.info("STEAL BASE(" + identifier + "): activities F: " 
                    + fresh.size() + " W: " + /*wrongContext.size() +*/ " R: " 
                    + runnable.size() + " L: " + lookup.size());
        }
        
        /*
        ActivityRecord r = wrongContext.steal(context);

        if (r == null){ 
            r = fresh.steal(context);
        }
        */
        
        if (allowRestricted) { 

            ActivityRecord r = restricted.steal(context);
            
            if (r != null) { 

                if (r.isStolen()) { 
                    // TODO: fix this!
                    logger.warning("MAJOR EEP!: return stolen job " 
                            + identifier);
                }   

                lookup.remove(r.identifier());

                if (Debug.DEBUG_STEAL) {
                    logger.info("STOLEN " + r.identifier());
                }

                r.setStolen(true);

                return r;
            }
  
            // If restricted fails we try the regular queue 
        }
        
        ActivityRecord r = fresh.steal(context);
        
        if (r != null) { 

            if (r.isStolen()) { 
                // TODO: fix this!
            	System.err.println("XX MAJOR EEP!: return stolen job " 
                        + identifier);
            	logger.warning("MAJOR EEP!: return stolen job " 
                        + identifier);
            }   

            lookup.remove(r.identifier());

            if (Debug.DEBUG_STEAL) {
                logger.info("STOLEN " + r.identifier());
            }

            r.setStolen(true);

            return r;
        }
            
        return null;
    }


    public String printState() { 

        String tmp = "BASE contains " + lookup.size()
        + " activities " + runnable.size() + " runnable  " 
        + fresh.size() + " fresh " + /*wrongContext.size() +*/ " wrong ";

        /*
        if (lookup.size() > 0) { 

            for (ActivityIdentifier i : lookup.keySet()) { 

                ActivityRecord a = lookup.get(i);

                if (a != null) { 
                    tmp += " [ " + i + " " + a + " ] ";
                } else { 
                    tmp += " < " + i + " > ";
                }
            }
        }*/

        return tmp;
    }

    private void process(ActivityRecord tmp) { 

        long start, end;

     //   System.out.println("PROCESSING: " + tmp.identifier());
        
        tmp.activity.setExecutor(executor);

        current = tmp;

        if (PROFILE) {
            start = System.currentTimeMillis();
        }

        tmp.run();

        if (PROFILE) { 
            end = System.currentTimeMillis();

            computationTime += end - start;

            activitiesInvoked++;
        }

        if (tmp.needsToRun()) {

            //out.println("REQUEUE " + tmp.identifier().localName() + " at " + end  
            //        + " " + (end - start));

            runnable.insertFirst(tmp);

        } else if (tmp.isDone()) {

            //out.println("CANCEL " + tmp.identifier().localName() + " at " + end  
            //        + " " + (end - start));

            cancel(tmp.identifier());
        } 

        //else { 
        //  out.println("SUSPEND " + tmp.identifier().localName() + " at " + end  
        //          + " " + (end - start));
        //}

        current = null;
    }

    boolean process() {

        ActivityRecord tmp = dequeue();

        // NOTE: the queue is guaranteed to only contain activities that we can 
        //       run. Whenever new activities are added or the the context of 
        //       this cohort changes we filter out all activities that do not 
        //       match. 

        if (tmp != null) {
            process(tmp);
            return true;
        }

        return false;
    }

    /*
    protected ActivityRecord removeWrongContext() { 
        
        if (wrongContext.size() == 0) { 
            return null;
        }

        ActivityRecord tmp = (ActivityRecord) wrongContext.steal(Context.ANY);
        lookup.remove(tmp.identifier());
        return tmp;
    }
    */
    
    long getComputationTime() { 
        return computationTime;
    }

    long getActivitiesSubmitted() { 
        return activitiesSubmitted;
    }

    long getActivitiesAdded() { 
        return activitiesAdded;
    }

    long getWrongContextSubmitted() { 
        return wrongContextSubmitted;
    }

    long getWrongContextAdded() { 
        return wrongContextAdded;
    }

    long getWrongContextDicovered() { 
        return wrongContextDicovered;
    }

    long getActivitiesInvoked() { 
        return activitiesInvoked;
    }

    long getMessagesInternal() { 
        return messagesInternal;
    }

    long getMessagesExternal() { 
        return messagesExternal;
    }

    long getMessagesTime() { 
        return messagesTime;
    }

    long getSteals() { 
        return steals;
    }

    long getStealSuccess() { 
        return stealSuccess;
    }

    long getStolen() { 
        return stolenJobs;
    }

    public void printStatus() {
        System.out.println(identifier + ": " + lookup);
    }

    public CohortIdentifier identifier() {
        return identifier;
    }

    public boolean isMaster() {

        if (parent == null) { 
            return true;
        }

        return parent.isMaster();
    }

    public WorkerContext getContext() {
        return myContext;
    }

    public void setContext(WorkerContext c) {
        myContext = c;

        if (Debug.DEBUG_CONTEXT) { 
            logger.info("Setting context of " + identifier + " (BASE) to " + c);
            logger.info("I have " + fresh.size() +" fresh and " 
                    + runnable.size() + " runnable activities");
        }

        // TODO: check status of local jobs 
        logger.fixme("CONTEXT CHANGED WITHOUT CHECKING JOBS FIX FIX FIX!", new Exception());
    }

    public void setContext(CohortIdentifier id, WorkerContext context) throws Exception {

        if (Debug.DEBUG_CONTEXT) { 
            logger.info("Setting context of BASE to " + context);
        }

        if (id.equals(identifier)) { 
            setContext(context);
            return;
        }

        throw new Exception("Cannot change context of " + id);
    }


    public void clearContext() {
        myContext = UnitWorkerContext.DEFAULT;
    }

    public Cohort[] getSubCohorts() {
        return null;
    }

    public boolean activate() { 

        if (parent != null) { 
            return true;
        }

        while (process());

        return false;
    }

    public CohortIdentifier[] getLeafIDs() {
        return new CohortIdentifier [] { identifier };
    }
    
    // NEW INTERFACE!
/*
    public void registerContext(Executor executor, WorkerContext c) {
    	myContext = c;
    	contextChange = true;
    }

	public void registerPool(Executor executor, StealPool myPool,
			boolean poolIsFixed) {
		parent.registerPool(executor, myPool, poolIsFixed);
	}

	public void registerStealPool(Executor executor, StealPool stealsFrom,
			boolean stealIsFixed) {
		parent.registerStealPool(executor, stealsFrom, stealIsFixed);
	}
*/
    
	public boolean processActitivies() {
		return parent.processActivities();
	}

	public void runExecutor() {
		
		try { 
			executor.run();
		} catch (Exception e) {
			logger.error("Executor terminated unexpectedly!", e);
		}		
	}

	public StealPool belongsTo() {
		return executor.belongsTo();
	}

	public StealPool stealsFrom() {
		return executor.stealsFrom();
	}
}
