package ibis.cohort.impl.multithreaded;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Cohort;
import ibis.cohort.Event;
import ibis.cohort.MessageEvent;

import java.util.ArrayList;
import java.util.HashMap;

public class Sequential implements Cohort {

    private HashMap<ActivityIdentifier, ActivityRecord> local = 
        new HashMap<ActivityIdentifier, ActivityRecord>();

    private CircularBuffer fresh = new CircularBuffer(16);    
    private ArrayList<ActivityRecord> runnable = new ArrayList<ActivityRecord>();    

    private final MTCohort parent;
    private final int workerID;

    private IDGenerator generator;

    Sequential(MTCohort parent, int workerID) { 
        this.parent = parent;
        this.workerID = workerID;
        this.generator = parent.getIDGenerator();
    }

    public void cancel(ActivityIdentifier id) {

        ActivityRecord ar = local.remove(id);

        if (ar == null) { 
            return;
        } 

        //System.out.println("CANCEL " + ar.activity);

        if (ar.needsToRun()) { 
            runnable.remove(ar);
        }
    }

    public void cancelAll() {

        if (local.size() == 0) { 
            return;
        }

        local.clear();
        runnable.clear();
    }

    public void done() {
        System.out.println("Quiting Cohort with " + local.size() + " activities in queue");
    }

    private ActivityRecord dequeue() {

        int size = runnable.size(); 

        if (size > 0) { 
            return runnable.remove(size-1);
        }

        if (!fresh.empty()) { 
            return (ActivityRecord) fresh.removeLast();
        }

        return null;
    }

    private ActivityIdentifier createActivityID() { 

        try { 
            return generator.createActivityID();
        } catch (Exception e) {
            // Oops, we ran out of IDs. Get some more from our parent!
            generator = parent.getIDGenerator();
        }

        try { 
            return generator.createActivityID();
        } catch (Exception e) { 
            throw new RuntimeException("ITERNAL ERROR: failed to create new ID block!", e);
        }

        //return new MTIdentifier(nextID++);
    }

    public ActivityIdentifier prepareSubmission(Activity a) { 

        ActivityIdentifier id = createActivityID();
        a.initialize(id);
        return id;
    }

    public void finishSubmission(Activity a) { 

        ActivityRecord ar = new ActivityRecord(a);
        local.put(a.identifier(), ar);
        fresh.insertLast(ar); 
    }

    void addActivityRecord(ActivityRecord a) { 
        local.put(a.identifier(), a);

        if (a.isFresh()) { 
            fresh.insertLast(a);
        } else { 
            runnable.add(a);
        }
    }

    public ActivityIdentifier submit(Activity a) {

        ActivityIdentifier id = prepareSubmission(a);
        finishSubmission(a);
        return id;
    }

    public void send(ActivityIdentifier source, ActivityIdentifier target, Object o) {

        ActivityRecord ar = local.get(target);

        if (ar == null) { 
            // Send isn't local, so forward to parent.
            parent.send(source, target, o);
        } else { 
            MessageEvent e = new MessageEvent(source, target, o);

            ar.enqueue(e);

            boolean change = ar.setRunnable();

            if (change) {     
                runnable.add(ar);
            }
        }
    } 

    public boolean queueEvent(Event e) {

        ActivityRecord ar = local.get(e.target);

        if (ar == null) { 
            return false;
        }

        //  System.out.println("   SEND " + source + " -> " + target + " (" + ar.activity + ") " + o);
        ar.enqueue(e);

        boolean change = ar.setRunnable();

        if (change) {     
            runnable.add(ar);
        }

        return true;
    } 

    void steal() {

        int size = fresh.size();

        if (size > 0) { 

            // Get the first of the new jobs (this is assumed to be the largest one)
            // remove it from our administration, and hand it over to our parent. 
            ActivityRecord r = (ActivityRecord) fresh.removeFirst();
         //   System.out.println("STEAL " + size + " " + r.identifier());
            local.remove(r.identifier());
            parent.stealReply(r);
        }
    }

    boolean process() { 

        ActivityRecord tmp = dequeue();

        if (tmp != null) {

            // System.out.println(workerID + ": Running " + tmp.identifier());

            tmp.activity.setCohort(this);
            tmp.run();

            if (tmp.needsToRun()) { 
                runnable.add(tmp);
            } else if (tmp.isDone()) { 
                cancel(tmp.identifier());
            }

            return true;
        }

        return false;
    }

    public void printStatus() {
        System.out.println(workerID + ": " + local);
    }
}
