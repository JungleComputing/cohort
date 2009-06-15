package ibis.cohort.impl.multithreaded;

import java.util.ArrayList;
import java.util.LinkedList;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Cohort;
import ibis.cohort.Event;
import ibis.cohort.MessageEvent;

public class ComputationUnit implements Cohort, Runnable {

    private boolean done;
    private Sequential sequential; 

    private ArrayList<Activity> pendingSubmit = new ArrayList<Activity>();
    private ArrayList<Event> pendingEvents = new ArrayList<Event>();
    private ArrayList<ActivityIdentifier> pendingCancelations = 
        new ArrayList<ActivityIdentifier>();

    private boolean cancelAll = false;
    private int stealRequests = 0;

    private volatile boolean commandPending = false;
   
    ComputationUnit(MTCohort parent, int workerID) { 
        sequential = new Sequential(parent, workerID);
    }

    public void cancel(ActivityIdentifier id) {
        // TODO: check pending submits first!

        synchronized (this) {   
            pendingCancelations.add(id);
        }

        commandPending = true; 
    }

    public void cancelAll() {
        synchronized (this) { 
            cancelAll = true;
        }
        commandPending = true;
    }

    public void stealRequest() {
        synchronized (this) {
            stealRequests++;
        }
        commandPending = true;
    }

    public void send(ActivityIdentifier source, ActivityIdentifier target, Object o) {
        MessageEvent me = new MessageEvent(source, target, o);

        synchronized (this) {
            pendingEvents.add(me);
        }

        commandPending = true;
    }

    public ActivityIdentifier submit(Activity a) {

        ActivityIdentifier id = sequential.prepareSubmission(a);

        synchronized (this) {
            pendingSubmit.add(a);
        }

        commandPending = true;

        return id;
    }

    private synchronized boolean getDone() { 
        return done;
    }

    public synchronized void done() { 
        done = true;
    }

    private synchronized Event dequeueEvent() { 

        final int size = pendingEvents.size();
        
        if (size > 0) { 
            return pendingEvents.remove(size-1);
        }

        return null;
    }
    
    private synchronized Activity dequeueSubmit() { 

        // TODO: Not sure if reversing the order is a good idea here!!!
        
        final int size = pendingSubmit.size();
        
        if (size > 0) { 
            return pendingSubmit.remove(size-1);
        }

        return null;
    }
    
    private synchronized ActivityIdentifier dequeueCancelation() { 

        final int size = pendingCancelations.size();
        
        if (size > 0) { 
            return pendingCancelations.remove(size-1);
        }

        return null;
    }

    private synchronized void processNextCommands() { 

        if (cancelAll) { 
            // Clear all tasks and events from the system.
            sequential.cancelAll();
            pendingSubmit.clear();
            pendingEvents.clear();
            pendingCancelations.clear();
            stealRequests = 0;
            
            cancelAll = false; 
            commandPending = false;
            return;
        }
      
        if (pendingSubmit.size() > 0) { 

            for (int i=0;i<pendingSubmit.size();i++) { 
                sequential.finishSubmission(pendingSubmit.get(i));
            }

            pendingSubmit.clear();
        } 
        
        if (pendingEvents.size() > 0) {

            for (int i=0;i<pendingEvents.size();i++) { 
                sequential.queueEvent(pendingEvents.get(i));
            }

            pendingEvents.clear();
        }
        
        if (pendingCancelations.size() > 0) { 

            for (int i=0;i<pendingCancelations.size();i++) { 
                sequential.cancel(pendingCancelations.get(i));
            }

            pendingCancelations.clear();
        }
        
        if (stealRequests > 0) { 

            for (int i=0;i<stealRequests;i++) { 
                sequential.steal();
            }

            stealRequests = 0;
        }
        
        commandPending = false;
    }

    public void run() {

        // NOTE: For D&C applications it seems to be most efficient to 
        // process a single command (i.e., a submit or an event) and then 
        // process all changes that occurred in the activities. 

        while (!getDone()) { 

            processNextCommands();
            
            boolean more = sequential.process();

            while (more && !commandPending) { 
                more = sequential.process();
            }
            
            if (!more && !commandPending) { 
                System.out.println("IDLE!");
                try { 
                    Thread.sleep(250);
                } catch (Exception e) {
                    // ignored
                }
            }
            
        }

        // System.out.println("ProcessCount " + processCount);
    }    
}
