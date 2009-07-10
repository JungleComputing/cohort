package ibis.cohort.impl.distributed;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Cohort;
import ibis.cohort.CohortIdentifier;
import ibis.cohort.Context;
import ibis.cohort.Event;
import ibis.cohort.MessageEvent;

import java.util.ArrayList;

public class SingleThreadedCohort implements Cohort, Runnable {

    private final MultiThreadedCohort parent; 
    private final BaseCohort sequential; 
    private final CohortIdentifier identifier;
    
    private static class PendingRequests { 
        final ArrayList<Activity> pendingSubmit = new ArrayList<Activity>();
        final ArrayList<Event> pendingEvents = new ArrayList<Event>();
        final ArrayList<Event> deliveredEvents = new ArrayList<Event>();
        final ArrayList<ActivityIdentifier> pendingCancelations = 
            new ArrayList<ActivityIdentifier>();   
    
        boolean cancelAll = false;
        int stealRequests = 0;
    } 
    
    private PendingRequests incoming = new PendingRequests();
    private PendingRequests processing = new PendingRequests();

    private boolean done = false;

    private volatile boolean havePendingRequests = false;
   
    SingleThreadedCohort(MultiThreadedCohort parent, CohortIdentifier identifier) { 
        this.parent = parent;
        this.identifier = identifier;
        sequential = new BaseCohort(parent, identifier);
    }

    public void cancel(ActivityIdentifier id) {
        // TODO: check pending submits first!

        synchronized (this) {   
            incoming.pendingCancelations.add(id);
        }

        havePendingRequests = true; 
    }

    public void stealRequest(CohortIdentifier source) {
      
        synchronized (this) {
            incoming.stealRequests++;
        }
        
        havePendingRequests = true;
    }

    public void deliverEvent(Event e) {

        synchronized (this) {
            incoming.deliveredEvents.add(e);
        }

        havePendingRequests = true;
    }
    
    public ActivityIdentifier submit(Activity a) {

     //   System.out.println("ST submit");
        
        ActivityIdentifier id = sequential.prepareSubmission(a);

        synchronized (this) {
            incoming.pendingSubmit.add(a);
        }

        havePendingRequests = true;

        return id;
    }

    public void send(ActivityIdentifier source, ActivityIdentifier target, Object o) {
        
        synchronized (this) {
            incoming.pendingEvents.add(new MessageEvent(source, target, o));
        }

        havePendingRequests = true;
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
                    parent.forwardEvent(e);
                }
            }

            processing.pendingEvents.clear();
        }
        
        if (processing.deliveredEvents.size() > 0) {

            for (int i=0;i<processing.deliveredEvents.size();i++) {
                
                Event e = processing.deliveredEvents.get(i);
                
                if (!sequential.queueEvent(e)) { 
                    // Failed to deliver event locally, so dispatch to parent 
                    System.err.println("EEP: Cohort " + identifier + " failed to deliver event: " + e);
                    new Exception().printStackTrace(System.err);
                    System.exit(1);
                }
            }

            processing.deliveredEvents.clear();
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

        long start = System.currentTimeMillis();
        
        while (!getDone()) { 

            processNextCommands();
            
            boolean more = sequential.process();

            while (more && !havePendingRequests) {
                more = sequential.process();
            }
            
            if (!more && !havePendingRequests) { 
                
                ActivityRecord r = parent.stealAttempt(identifier);
                
                if (r != null) { 
                    
                    ((DistributedActivityIdentifier) 
                            r.identifier()).setLastKnownCohort(
                                    (DistributedCohortIdentifier) identifier);
                    
                    sequential.addActivityRecord(r);
                } else  {
                    //System.out.println(identifier + ": STEAL FAIL -- IDLE!");
                    
                    try {
                        Thread.sleep(100);
                    } catch (Exception e) {
                       // ignored
                    }
                }
            }
            
        }
        
        long time = System.currentTimeMillis() - start;

        sequential.printStatistics(time);
    }

    public CohortIdentifier identifier() {
        return identifier;
    }    
    
    public boolean isMaster() {
        return parent.isMaster();
    }

    public Context getContext() {
        // TODO Auto-generated method stub
        return null;
    }

    public void setContext(Context context) {
        // TODO Auto-generated method stub
        
    }
    
    
}
