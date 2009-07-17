package ibis.cohort.impl.distributed;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Cohort;
import ibis.cohort.CohortIdentifier;
import ibis.cohort.Context;
import ibis.cohort.Event;
import ibis.cohort.MessageEvent;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;

public class SingleThreadedCohort implements Cohort, Runnable {

    private static final boolean PROFILE = true;

    private final ThreadMXBean management;

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

    private long sleepTime;
    private long activeTime;
    private long commandTime;
    private long stealTime;
    
    private volatile boolean havePendingRequests = false;
   
    SingleThreadedCohort(MultiThreadedCohort parent, CohortIdentifier identifier) { 
        
        if (PROFILE) { 
            management = ManagementFactory.getThreadMXBean();

            if (management.isThreadCpuTimeSupported() && 
                    !management.isThreadCpuTimeEnabled()) { 
                management.setThreadCpuTimeEnabled(true);
            } 

            if (management.isThreadContentionMonitoringSupported() && 
                    !management.isThreadContentionMonitoringEnabled()) { 
                management.setThreadContentionMonitoringEnabled(true);
            }
        }
        
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

            long t1 = System.currentTimeMillis();
            
            processNextCommands();
            
            long t2 = System.currentTimeMillis();
            
            boolean more = sequential.process();
            
            while (more && !havePendingRequests) {
                more = sequential.process();
            }

            long t3 = System.currentTimeMillis();
            
            if (!more && !havePendingRequests) { 
                
                ActivityRecord r = parent.stealAttempt(identifier);
                
                long t4 = System.currentTimeMillis();
                
                if (r != null) { 
                    
                    ((DistributedActivityIdentifier) 
                            r.identifier()).setLastKnownCohort(
                                    (DistributedCohortIdentifier) identifier);
                    
                    sequential.addActivityRecord(r);

                    stealTime += t4 - t3;
                
                } else  {
                    //System.out.println(identifier + ": STEAL FAIL -- IDLE!");
                    
                    try {
                        Thread.sleep(100);
                    } catch (Exception e) {
                       // ignored
                    }

                    long t5 = System.currentTimeMillis();
                
                    sleepTime += t5 - t4;
                }
            }
            
            commandTime += t2-t1;
            activeTime += t3-t2;
        }
        
        long time = System.currentTimeMillis() - start;

        printStatistics(time);
    }

    public void printStatistics(long totalTime) {

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
        
        final long computationTime = sequential.getComputationTime();
        final long activitiesInvoked = sequential.getActivitiesInvoked();
        final long activitiesSubmitted = sequential.getActivitiesSubmitted();
        
        final long messagesInternal = sequential.getMessagesInternal();
        final long messagesExternal = sequential.getMessagesExternal();
        
        final long steals = sequential.getSteals();
        
        final double comp = (100.0 * computationTime) / totalTime;
        final double fact = ((double) activitiesInvoked) / activitiesSubmitted;
        
        final double stealPerc = (100.0 * stealTime) / totalTime;
        final double commandPerc = (100.0 * commandTime) / totalTime;
        final double activePerc = (100.0 * activeTime) / totalTime;
        final double sleepPerc = (100.0 * sleepTime) / totalTime;
        
        if (PROFILE) { 
            // Get the cpu/user time (in nanos) 
            cpuTime = management.getCurrentThreadCpuTime();
            userTime = management.getCurrentThreadUserTime();
            
            
            cpuPerc = (cpuTime / 10000.0) / totalTime;
            userPerc = (userTime / 10000.0) / totalTime;
       
            cpuTime = cpuTime / 1000000L;
            userTime = userTime / 1000000L;
            
            ThreadInfo info = 
                management.getThreadInfo(Thread.currentThread().getId());
            
            blocked = info.getBlockedCount();
            blockedTime = info.getBlockedTime();
            
            waited = info.getWaitedCount();
            waitedTime = info.getWaitedTime();
            
            blockedPerc = (100.0 * blockedTime) / totalTime;
            waitedPerc = (100.0 * waitedTime) / totalTime;
        }
        
        synchronized (System.out) {

            System.out.println(identifier + " statistics");
            System.out.println(" Time");
            System.out.println("   total      : " + totalTime + " ms.");
            System.out.println("   active     : " + activeTime + " ms. ("
                    + activePerc + " %)");
            System.out.println("        run() : " + computationTime + " ms. ("
                    + comp + " %)");

            System.out.println("   command    : " + commandTime + " ms. ("
                    + commandPerc + " %)");

            System.out.println("   sleep      : " + sleepTime + " ms. ("
                    + sleepPerc + " %)");

            System.out.println("   steal      : " + stealTime + " ms. ("
                    + stealPerc + " %)");
            
            if (PROFILE) { 

                System.out.println("   cpu time   : " + cpuTime + " ms. ("
                        + cpuPerc + " %)");

                System.out.println("   user time  : " + userTime + " ms. ("
                        + userPerc + " %)");

                System.out.println("   blocked    : " + blocked + " times");

                System.out.println("   block time : " + blockedTime+ " ms. ("
                        + blockedPerc + " %)");

                System.out.println("   waited     : " + waited + " times");

                System.out.println("   wait time  : " + waitedTime+ " ms. ("
                        + waitedPerc + " %)");

            }
            
            System.out.println(" Activities");
            System.out.println("   submitted  : " + activitiesSubmitted);
            System.out.println("   invoked    : " + activitiesInvoked + " ("
                    + fact + " /act)");
            System.out.println(" Messages");
            System.out.println("   internal   : " + messagesInternal);
            System.out.println("   external   : " + messagesExternal);
            System.out.println(" Steals");
            System.out.println("   incoming   : " + steals);
        }
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
