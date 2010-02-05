package ibis.cohort.impl.distributed.single;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.ActivityIdentifierFactory;
import ibis.cohort.Cohort;
import ibis.cohort.CohortIdentifier;
import ibis.cohort.Context;
import ibis.cohort.Event;
import ibis.cohort.MessageEvent;
import ibis.cohort.extra.CircularBuffer;
import ibis.cohort.extra.Log;
import ibis.cohort.impl.distributed.ActivityRecord;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Properties;

public class BaseCohort implements Cohort {

    private static final boolean DEBUG = false;
    
    private final SingleThreadedBottomCohort parent;

    private final CohortIdentifier identifier;

    private PrintStream out;
    private final Log logger;
    
    // Default context is ANY
    private Context myContext = Context.ANY;
    
    private HashMap<String, ActivityIdentifier> registry = 
        new HashMap<String, ActivityIdentifier>();
    
    private HashMap<ActivityIdentifier, ActivityRecord> lookup = 
        new HashMap<ActivityIdentifier, ActivityRecord>();

    // TODO: replace by more efficient datastructure
    private CircularBuffer wrongContext = new CircularBuffer(1);
    
    private CircularBuffer fresh = new CircularBuffer(1);

    private CircularBuffer runnable = new CircularBuffer(1);

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
    
    private long messagesInternal;
    private long messagesExternal;
    private long messagesTime;

    private ActivityRecord current;
    
    BaseCohort(SingleThreadedBottomCohort parent, Properties p, 
            CohortIdentifier identifier, PrintStream out, Log logger) {
        this.parent = parent;
        this.identifier = identifier;
        this.generator = parent.getActivityIdentifierFactory(identifier);
        this.logger = logger;
        this.out = out;
    }
    
    public BaseCohort(Properties p) {
        this.parent = null;
        this.identifier = new CohortIdentifier(0);
        
        this.generator = new ActivityIdentifierFactory(0, 0, Long.MAX_VALUE);
        
        String outfile = p.getProperty("ibis.cohort.outputfile");
        
        if (outfile != null) {
            String filename = outfile + "." + identifier;
            
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
        
        logger = new Log(identifier + " [SEQ] ", out, DEBUG);
    }        
    
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

    protected CircularBuffer getWrongContextQueue() { 
        return wrongContext;
    }
    
    private ActivityRecord dequeue() {

        int size = runnable.size();

        if (size > 0) {
            return (ActivityRecord) runnable.removeFirst();
        }

        if (!fresh.empty()) {
            
            ActivityRecord tmp = (ActivityRecord) fresh.removeLast();
            
           // DistributedActivityIdentifier id = 
           //     (DistributedActivityIdentifier) tmp.activity.identifier();
           // id.setLastKnownCohort((DistributedCohortIdentifier) identifier);
            return tmp;
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

        if (DEBUG) {
            logger.info("created " + id + " at " 
                + System.currentTimeMillis() + " from " 
                + (current == null ? "ROOT" : current.identifier()));
        }
        return id;
    }

    public void finishSubmission(Activity a) {

        activitiesSubmitted++;
       
      //  if (activitiesSubmitted % 10000 == 0) { 
      //      System.out.println("BASE(" + identifier + ") submit " + a.identifier() + " " + activitiesSubmitted);
      //  }
        
        ActivityRecord ar = new ActivityRecord(a);

        lookup.put(a.identifier(), ar);

        Context c = a.getContext();
        
        if (c.isAny() || c.isLocal() || myContext.contains(c)) { 
        
           // System.out.println("BASE(" + identifier + ") submit " + a.identifier() + " COMPLETED");
            
            fresh.insertLast(ar);
        } else {
          
            if (DEBUG) { 
                logger.info("submitted " + a.identifier() + " with WRONG CONTEXT");
            }   
            wrongContextSubmitted++;
            wrongContext.insertLast(ar);
        }
    }

    void addActivityRecord(ActivityRecord a) {
        
        if (DEBUG) {
            logger.info("received " + a.identifier() + " at " 
                + System.currentTimeMillis());
        }
        
        activitiesAdded++;
        
        lookup.put(a.identifier(), a);

        Context c = a.activity.getContext();
        
        if (c.isAny() || c.isLocal() || myContext.contains(c)) { 
            if (a.isFresh()) {
                fresh.insertLast(a);
            } else {
                runnable.insertLast(a);
            }
        } else {
            wrongContextAdded++;
            wrongContext.insertLast(a);
        }
    }

    protected ActivityRecord lookup(ActivityIdentifier id) { 
        return lookup.get(id);
    }
    
    public ActivityIdentifier submit(Activity a) {

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
    
    public ActivityIdentifier lookup(String name, Context scope) {
        
        if (parent == null || scope.isLocal()) { 
            return lookup(name);
        }
        
        return parent.lookup(name, scope);
    }    
    
    public boolean register(String name, ActivityIdentifier id, Context scope) {
        
        if (parent == null || scope.isLocal()) { 
            return register(name, id);
        }
        
        return parent.register(name, id, scope);
    }
    
    public boolean deregister(String name, Context scope) {

        if (parent == null || scope.isLocal()) { 
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
        
        if (DEBUG) {
            start = System.currentTimeMillis();
           
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
        
        if (DEBUG) {       
            end = System.currentTimeMillis();
            messagesTime += (end-start); 
        }
    }

    public boolean queueEvent(Event e) {

        ActivityRecord ar = lookup.get(e.target);

        if (ar == null) {
            if (DEBUG) { 
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
  
    int available() { 
        return fresh.size();
    }
    
    ActivityRecord steal(Context context) {

    //    System.out.println("STEAL BASE(" + identifier + "): activities " 
    //            + fresh.size() + " " + wrongContext.size() + " ");
        
        steals++;

        int size = wrongContext.size();
        
        if (size > 0) {

            for (int i=0;i<size;i++) { 
                // Get the first of the jobs (this is assumed to be the 
                // largest one) and check if we are allowed to return it. 
                ActivityRecord r = (ActivityRecord) wrongContext.get(i);
                
                if (!r.isStolen()) { 

                    Context tmp = r.activity.getContext();

                    if (tmp.contains(context)) { 

                        wrongContext.remove(i);

                        lookup.remove(r.identifier());

                        stealSuccess++;

                        if (DEBUG) {
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

                    if (!tmp.isLocal() && tmp.contains(context)) { 

                        fresh.remove(i);

                        lookup.remove(r.identifier());

                        stealSuccess++;

                        if (DEBUG) {
                            logger.info("STOLEN " + r.identifier());
                        }

                        r.setStolen(true);

                        return r;
                    }
                }
            } 
        }
        
        return null;
    }

    public String printState() { 
        
        String tmp = "BASE contains " + lookup.size()
                + " activities " + runnable.size() + " runnable  " 
                + fresh.size() + " fresh";
        
        for (ActivityIdentifier i : lookup.keySet()) { 
           
            ActivityRecord a = lookup.get(i);
            
            if (a != null) { 
                tmp += " [ " + i + " " + a + " ] ";
            } else { 
                tmp += " < " + i + " > ";
            }
        }
        
        return tmp;
    }
    
    private void process(ActivityRecord tmp) { 

        logger.info("Processing " + tmp.activity.identifier());
        
        
        long start, end;
        
        tmp.activity.setCohort(this);

        current = tmp;
        
        if (DEBUG) {
            start = System.currentTimeMillis();
        }

        tmp.run();

        if (DEBUG) { 
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

        // NOTE: the queue is garanteed to only contain activities that we can 
        //       run. Whenever new activities are added or the the context of 
        //       this cohort changes we filter out all activities that do not 
        //       match. 
                       
        if (tmp != null) {
            process(tmp);
            return true;
        }
        
        return false;
    }
    
    protected ActivityRecord removeWrongContext() { 
        if (wrongContext.size() == 0) { 
            return null;
        }
        
        ActivityRecord tmp = (ActivityRecord) wrongContext.removeFirst();
        lookup.remove(tmp.identifier());
        return tmp;
    }
    
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

    public Context getContext() {
        return myContext;
    }

    public void setContext(Context c) {
        myContext = c;
        
        // TODO: check status of local jobs 
        logger.warning("CONTEXT CHANGED WITHOUT CHECKING JOBS FIX FIX FIX!", new Exception());
    }
    
    public void setContext(CohortIdentifier id, Context context) throws Exception {
        
        if (id.equals(identifier)) { 
            setContext(context);
            return;
        }
        
        throw new Exception("Cannot change context of " + id);
    }
    
    
    public void clearContext() {
        myContext = Context.ANY;
    }
    
    public PrintStream getOutput() {       
        return out;
    }

    public Cohort[] getSubCohorts() {
        return null;
    }
    
    public boolean activate() { 
     
        logger.info("Activate called: " + parent);
        
        if (parent != null) { 
            return true;
        }
        
        while (process()) { 
        }
        
        return false;
    }
   

   
}
