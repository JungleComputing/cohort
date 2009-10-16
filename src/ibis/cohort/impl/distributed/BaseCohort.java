package ibis.cohort.impl.distributed;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Cohort;
import ibis.cohort.CohortIdentifier;
import ibis.cohort.Context;
import ibis.cohort.Event;
import ibis.cohort.MessageEvent;

import java.io.PrintStream;
import java.util.HashMap;

class BaseCohort implements Cohort {

    private static final boolean DEBUG = false;
    
    private final MultiThreadedCohort parent;

    private final CohortIdentifier identifier;

    private final PrintStream out;
    
    private Context context;
    
    private HashMap<ActivityIdentifier, ActivityRecord> local = 
        new HashMap<ActivityIdentifier, ActivityRecord>();

    private CircularBuffer fresh = new CircularBuffer(1);

    private CircularBuffer runnable = new CircularBuffer(1);

    private DistributedActivityIdentifierGenerator generator;
    
    private long computationTime;
    
    private long activitiesSubmitted;

    private long activitiesInvoked;

    private long steals;
    private long stealSuccess;
    
    private long messagesInternal;
    private long messagesExternal;
    private long messagesTime;

    private ActivityRecord current;
    
    BaseCohort(MultiThreadedCohort parent, CohortIdentifier identifier, 
            PrintStream out) {
        this.parent = parent;
        this.identifier = identifier;
        this.generator = parent.getIDGenerator(identifier);
        this.out = out;
        
        // default context is "ANY"
        context = Context.ANY;
    }
    
    public void cancel(ActivityIdentifier id) {

        ActivityRecord ar = local.remove(id);

        if (ar == null) {
            return;
        }

        // System.out.println("CANCEL " + ar.activity);

        if (ar.needsToRun()) {
            runnable.remove(ar);
        }
    }

    public void done() {
        System.out.println("Quiting Cohort with " + local.size()
                + " activities in queue");
    }

    private ActivityRecord dequeue() {

        int size = runnable.size();

        if (size > 0) {
            return (ActivityRecord) runnable.removeFirst();
        }

        if (!fresh.empty()) {
            
            ActivityRecord tmp = (ActivityRecord) fresh.removeLast();
            
            DistributedActivityIdentifier id = 
                (DistributedActivityIdentifier) tmp.activity.identifier();
            id.setLastKnownCohort((DistributedCohortIdentifier) identifier);
            return tmp;
        }

        return null;
    }

    private ActivityIdentifier createActivityID() {

        try {
            return generator.createActivityID();
        } catch (Exception e) {
            // Oops, we ran out of IDs. Get some more from our parent!
            generator = parent.getIDGenerator(identifier);
        }

        try {
            return generator.createActivityID();
        } catch (Exception e) {
            throw new RuntimeException(
                    "ITERNAL ERROR: failed to create new ID block!", e);
        }

        // return new MTIdentifier(nextID++);
    }

    public ActivityIdentifier prepareSubmission(Activity a) {
        
        ActivityIdentifier id = createActivityID();
        a.initialize(id);

        if (DEBUG) {
            out.println("CREATE " + id.localName() + " at " 
                + System.currentTimeMillis() + " from " 
                + (current == null ? "ROOT" : current.identifier().localName()));
        }
        return id;
    }

    public void finishSubmission(Activity a) {

        ActivityRecord ar = new ActivityRecord(a);
       
        local.put(a.identifier(), ar);
       
        fresh.insertLast(ar);
        activitiesSubmitted++;
    }

    void addActivityRecord(ActivityRecord a) {
        
        local.put(a.identifier(), a);

        if (a.isFresh()) {
            fresh.insertLast(a);
            activitiesSubmitted++;
        } else {
            runnable.insertLast(a);
        }
    }

    public ActivityIdentifier submit(Activity a) {

        ActivityIdentifier id = prepareSubmission(a);
        finishSubmission(a);
        return id;
    }

    public void send(ActivityIdentifier source, ActivityIdentifier target,
            Object o) {
        
        long start, end;
        
        if (DEBUG) {
            start = System.currentTimeMillis();
            out.println("SEND " + source.localName() + " to " 
                    + target.localName() + " at " + start);
        }
        
        MessageEvent e = new MessageEvent(source, target, o);
        
        ActivityRecord ar = local.get(target);

        if (ar == null) {
            // Send isn't local, so forward to parent.

            messagesExternal++;

            parent.forwardEvent(e);

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

        ActivityRecord ar = local.get(e.target);

        if (ar == null) {
            
            System.err.println("EEP: failed to find " + e.target);
            
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

        steals++;

        int size = fresh.size();

        if (size > 0) {

            for (int i=0;i<size;i++) { 
                // Get the first of the new jobs (this is assumed to be the 
                // largest one) and check if we are allowed to return it. 
                ActivityRecord r = (ActivityRecord) fresh.get(i);
                
                if (!r.isStolen()) { 


                    Context tmp = r.activity.getContext();

                    if (tmp != Context.LOCAL && tmp.match(context)) { 

                        fresh.remove(i);

                        local.remove(r.identifier());

                        stealSuccess++;

                        if (DEBUG) {
                            out.println("STOLEN " + r.identifier().localName());
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
        
        String tmp = "BASE contains " + local.size()
                + " activities " + runnable.size() + " runnable  " 
                + fresh.size() + " fresh";
        
        for (ActivityIdentifier i : local.keySet()) { 
           
            ActivityRecord a = local.get(i);
            
            if (a != null) { 
                tmp += " [ " + i.localName()  + " " + a + " ] ";
            } else { 
                tmp += " < " + i.localName() + " > ";
            }
        }
        
        return tmp;
    }
    
    boolean process() {
        
        long start, end;
        
        ActivityRecord tmp = dequeue();

        if (tmp != null) {

            tmp.activity.setCohort(this);

            current = tmp;
            
            if (DEBUG) {
                start = System.currentTimeMillis();
                out.println("RUN " + tmp.identifier().localName() + " at " + start);
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
            
                if (DEBUG) {
                    out.println("CANCEL " + tmp.identifier().localName() + " at " + end);
                }
                
                //out.println("CANCEL " + tmp.identifier().localName() + " at " + end  
                //        + " " + (end - start));
                
                cancel(tmp.identifier());
            } 
            
            //else { 
            //  out.println("SUSPEND " + tmp.identifier().localName() + " at " + end  
            //          + " " + (end - start));
            //}

            current = null;
            
            return true;
        }

        return false;
    }

    long getComputationTime() { 
        return computationTime;
    }

    long getActivitiesSubmitted() { 
        return activitiesSubmitted;
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
        System.out.println(identifier + ": " + local);
    }

    public CohortIdentifier identifier() {
        return identifier;
    }

    public boolean isMaster() {
        return parent.isMaster();
    }

    public Context getContext() {
        return context;
    }

    public void setContext(Context context) {
        this.context = context;
    }

    public PrintStream getOutput() {
        return out;
    }
}
