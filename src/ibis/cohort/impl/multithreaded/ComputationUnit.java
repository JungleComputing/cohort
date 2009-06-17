package ibis.cohort.impl.multithreaded;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Cohort;
import ibis.cohort.Event;
import ibis.cohort.MessageEvent;

import java.util.ArrayList;

public class ComputationUnit implements Cohort, Runnable {

    private final MTCohort parent; 
    private final Sequential sequential; 
    private final int workerID;
    
    private static class PendingRequests { 
        final ArrayList<Activity> pendingSubmit = new ArrayList<Activity>();
        final ArrayList<Event> pendingEvents = new ArrayList<Event>();
        final ArrayList<ActivityIdentifier> pendingCancelations = 
            new ArrayList<ActivityIdentifier>();   
    
        boolean cancelAll = false;
        int stealRequests = 0;
    } 
    
    private PendingRequests incoming = new PendingRequests();
    private PendingRequests processing = new PendingRequests();

    private boolean done = false;

    private volatile boolean havePendingRequests = false;
   
    ComputationUnit(MTCohort parent, int workerID) { 
        this.parent = parent;
        this.workerID = workerID;
        sequential = new Sequential(parent, workerID);
    }

    public void cancel(ActivityIdentifier id) {
        // TODO: check pending submits first!

        synchronized (this) {   
            incoming.pendingCancelations.add(id);
        }

        havePendingRequests = true; 
    }

    public void cancelAll() {
        synchronized (this) { 
            incoming.cancelAll = true;
        }
        havePendingRequests = true;
    }

    public void stealRequest() {
        synchronized (this) {
            incoming.stealRequests++;
        }
        havePendingRequests = true;
    }

    public void queueEvent(Event e) {

        synchronized (this) {
            incoming.pendingEvents.add(e);
        }

        havePendingRequests = true;
    }
    
    public ActivityIdentifier submit(Activity a) {

        ActivityIdentifier id = sequential.prepareSubmission(a);

        synchronized (this) {
            incoming.pendingSubmit.add(a);
        }

        havePendingRequests = true;

        return id;
    }

    public void send(ActivityIdentifier source, ActivityIdentifier target, Object o) {
        queueEvent(new MessageEvent(source, target, o));
    }
    
    private synchronized boolean getDone() { 
        return done;
    }

    public synchronized void done() { 
        done = true;
    }

    private synchronized void swapPendingRequests() {         
        PendingRequests tmp = incoming;
        incoming = processing;
        processing = tmp;        
        havePendingRequests = false;
    }
    
    private void processNextCommands() { 

        swapPendingRequests();
        
        if (processing.cancelAll) { 
            // Clear all tasks and events from the system.
            sequential.cancelAll();
            
            processing.pendingSubmit.clear();
            processing.pendingEvents.clear();
            processing.pendingCancelations.clear();
            processing.stealRequests = 0;
            
            processing.cancelAll = false; 
            
            // TODO: We should also clear the other PendingRequest object ?            
            return;
        }
      
        if (processing.pendingSubmit.size() > 0) { 

            for (int i=0;i<processing.pendingSubmit.size();i++) { 
                sequential.finishSubmission(processing.pendingSubmit.get(i));
            }

            processing.pendingSubmit.clear();
        } 
        
        if (processing.pendingEvents.size() > 0) {

            for (int i=0;i<processing.pendingEvents.size();i++) {
                
                Event e = processing.pendingEvents.get(i);
                
                if (!sequential.queueEvent(e)) { 
                   // Failed to deliver event locally, so dispatch to parent 
                    parent.forwardEvent(e, workerID);
                }
            }

            processing.pendingEvents.clear();
        }
        
        if (processing.pendingCancelations.size() > 0) { 

            for (int i=0;i<processing.pendingCancelations.size();i++) { 
                sequential.cancel(processing.pendingCancelations.get(i));
            }

            processing.pendingCancelations.clear();
        }
        
        if (processing.stealRequests > 0) { 

            sequential.steal();
            
            processing.stealRequests = 0;
        }
    }

    public void run() {

        // NOTE: For D&C applications it seems to be most efficient to 
        // process a single command (i.e., a submit or an event) and then 
        // process all changes that occurred in the activities. 

        while (!getDone()) { 

            processNextCommands();
            
            boolean more = sequential.process();

            while (more && !havePendingRequests) { 
                more = sequential.process();
            }
            
            if (!more && !havePendingRequests) { 
                
                ActivityRecord r = parent.stealAttempt(workerID);
                
                if (r != null) { 
                    //System.out.println(workerID + ": STEAL SUCCESS " + r.identifier());
                    
                    sequential.addActivityRecord(r);
                } else  {
                    //System.out.println(workerID + ": STEAL FAIL -- IDLE!");
                    
                    try { 
                        Thread.sleep(10);
                    } catch (Exception e) {
                       // ignored
                    }
                }
            }
            
        }

        // System.out.println("ProcessCount " + processCount);
    }    
}
