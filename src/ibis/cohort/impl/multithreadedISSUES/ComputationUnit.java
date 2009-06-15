package ibis.cohort.impl.multithreadedISSUES;

import ibis.cohort.Cohort;
import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Event;
import ibis.cohort.MessageEvent;

import java.util.ArrayList;
import java.util.HashMap;

public class ComputationUnit implements Cohort, Runnable {

    private final MTCohort parent;
    private final int workerNumber; 

    private IDGenerator generator;

    private boolean done = false;

    private final HashMap<ActivityIdentifier, ActivityRecord> localActivities = 
        new HashMap<ActivityIdentifier, ActivityRecord>();

    private ArrayList<ActivityRecord> newActivities = new ArrayList<ActivityRecord>();    

    private ArrayList<ActivityRecord> pendingActivities = new ArrayList<ActivityRecord>();    
    private ArrayList<ActivityRecord> processingActivities = new ArrayList<ActivityRecord>();    

    private ArrayList<Event> pendingEvents = new ArrayList<Event>();
    private ArrayList<Event> processingEvents = new ArrayList<Event>();

    public ComputationUnit(MTCohort parent, int workerNumber) { 
        this.parent = parent;
        this.workerNumber = workerNumber;
        generator = parent.getIDGenerator();
    }

    public synchronized void cancel(ActivityIdentifier id) {

        // TODO: this implementation is incorrect!!!

        ActivityRecord ar = localActivities.remove(id);

        if (ar == null) { 
            return;
        } 

        //System.out.println("CANCEL " + ar.activity);

        if (ar.needsToRun()) { 
            pendingActivities.remove(ar);
        }

    }

    public void cancelAll() {
        // TODO: implement
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
    }

    public ActivityIdentifier submit(Activity a) {

        ActivityIdentifier id = createActivityID();

        a.initialize(this, id);

        ActivityRecord ar = new ActivityRecord(a);

        synchronized (this) {
            localActivities.put(a.identifier(), ar);
            newActivities.add(ar); 
        }

        return id;
    }

    public void send(ActivityIdentifier source, ActivityIdentifier target, Object o) {
        queueEvent(new MessageEvent(source, target, o));
    } 

    public synchronized void queueEvent(Event e) { 
        pendingEvents.add(e);
    }

    private synchronized  void queueActivity(ActivityRecord ar) { 
        pendingActivities.add(ar);
    }

    private synchronized ArrayList<Event> getPendingEvents() { 

        // Swap the pending/processing event list, so we can do 
        // a safe lock free iteration through the list
        ArrayList<Event> tmp = pendingEvents;
        pendingEvents = processingEvents;
        processingEvents = tmp;
        return tmp;
    }

    private synchronized ArrayList<ActivityRecord> getPendingActivities() { 

        // Swap the pending/processing event list, so we can do 
        // a safe lock free iteration through the list
        ArrayList<ActivityRecord> tmp = pendingActivities;
        pendingActivities = processingActivities;
        processingActivities = tmp;
        return tmp;
    }

    public synchronized ActivityRecord steal() { 

        int size = pendingActivities.size();

        if (size > 0) { 
            ActivityRecord r = pendingActivities.remove(pendingActivities.size()-1);
            localActivities.remove(r);
            return r;
        }

        return null;
    }

    public synchronized void done() { 
        done = true;
    }

    private synchronized boolean getDone() { 
        return done;
    }

    private boolean processEvents() { 

        ArrayList<Event> tmp = getPendingEvents();

        if (tmp.size() == 0) { 
            return false;
        }

        for (int i=0;i<tmp.size();i++) { 

            Event e = tmp.get(i);

            ActivityRecord ar = localActivities.get(e.target);

            if (ar != null) { 
                ar.enqueue(e);

                boolean change = ar.setRunnable();

                if (change) {     
                    queueActivity(ar);
                }            
            } else { 
                parent.deliverEvent(e, workerNumber);
            }
        }

        tmp.clear();

        return true;
    }

    private boolean processActivities() { 

        ArrayList<ActivityRecord> tmp = getPendingActivities();

        if (tmp.size() == 0) { 
            return false;
        }

        for (int i=0;i<tmp.size();i++) { 

            ActivityRecord ar = tmp.get(i);

            ar.run();

            if (ar.needsToRun()) { 
                queueActivity(ar);
            } else if (ar.isDone()) { 
                cancel(ar.identifier());
            }
        }

        tmp.clear();

        return true;
    }
    
    private boolean processNewActivities() { 

        ActivityRecord tmp = null;
        
        synchronized (this) {
            if (newActivities.size() == 0) { 
                return false;
            }
            
            tmp = newActivities.remove(newActivities.size()-1);
        }
        
       tmp.run();

       if (tmp.needsToRun()) { 
           queueActivity(tmp);
       } else if (tmp.isDone()) { 
           cancel(tmp.identifier());
       }

        return true;
    }

    private void stealAttempt() { 

        ActivityRecord tmp = parent.stealAttempt(workerNumber);

        if (tmp != null) { 

            synchronized (this) {
                localActivities.put(tmp.identifier(), tmp);    

                if (tmp.isRunnable()) { 
                    // Should always return true ?
                            queueActivity(tmp);
                }
            }
        }
    }

    private synchronized boolean hasWork() { 
        return (pendingEvents.size() > 0 || pendingActivities.size() > 0);
    }

    public void run() { 

        while (!getDone()) { 

            final boolean events = processEvents();
            final boolean activities = processActivities();
            final boolean fresh = processNewActivities();
            
            if (!events && !activities && !fresh) { 
                // Noting to do, so try stealing work from another thread
                stealAttempt();

                // If the also failed and we're sure we are idle we sleep for a while....
                if (!getDone() && !hasWork()) { 
                    try { 
                        System.out.println("SLEEP!");
                        Thread.sleep(250); // TODO: totally arbitrary number!
                    } catch (Exception e) {
                        // ignored
                    }
                }    
            }
        }

        // sanity check
        if (hasWork()) { 
            throw new RuntimeException("Quiting while there is work to do!");
        }
    }
}
