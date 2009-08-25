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

    private static final int [] SLEEP_TIMES = { 1, 1, 2, 5, 10, 20, 50, 100, 200, 1000 }; 
    
    private final ThreadMXBean management;

    private final MultiThreadedCohort parent;

    private final BaseCohort sequential;

    private final CohortIdentifier identifier;
    
    private final int workerID;
    
    private static class PendingRequests {
        final ArrayList<Activity> pendingSubmit = new ArrayList<Activity>();

        final ArrayList<Event> pendingEvents = new ArrayList<Event>();

        final ArrayList<Event> deliveredEvents = new ArrayList<Event>();

        final ArrayList<ActivityIdentifier> pendingCancelations = new ArrayList<ActivityIdentifier>();

        final ArrayList<StealRequest> stealRequests = new ArrayList<StealRequest>();
        
        boolean cancelAll = false;
    }

    private PendingRequests incoming = new PendingRequests();
    private PendingRequests processing = new PendingRequests();
    
    private StealRequest currentSteal;
    
    private int sleepIndex;
    
    private boolean done = false;

    private long sleepTime;
    private long sleepCount;
    
    private long activeTime;
    private long commandTime;

    private long stealTime;
    private long stealCount;
    private long stealSuccess;

    
    // NOTE: these are use for performance debugging...
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

    private volatile boolean havePendingRequests = false;

    SingleThreadedCohort(MultiThreadedCohort parent, int workerID, 
            CohortIdentifier identifier) {

        if (PROFILE) {
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
            }
        }

        this.parent = parent;
        this.workerID = workerID;
        this.identifier = identifier;
        sequential = new BaseCohort(parent, identifier);
    }

    public void cancel(ActivityIdentifier id) {

        synchronized (this) {
            incoming.pendingCancelations.add(id);
        }

        havePendingRequests = true;
    }
    
    public void postStealRequest(StealRequest s) {

        synchronized (this) {
            incoming.stealRequests.add(s);
      
            System.err.println("Worker " + workerID + " now has " 
                    + incoming.stealRequests.size() + " pending steals");
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

        // System.out.println("ST submit");

        ActivityIdentifier id = sequential.prepareSubmission(a);

        synchronized (this) {
            incoming.pendingSubmit.add(a);
        }

        havePendingRequests = true;

        return id;
    }

    public void send(ActivityIdentifier source, ActivityIdentifier target,
            Object o) {

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

            for (int i = 0; i < processing.pendingSubmit.size(); i++) {
                sequential.finishSubmission(processing.pendingSubmit.get(i));
            }

            processing.pendingSubmit.clear();
        }

        if (processing.pendingEvents.size() > 0) {

            for (int i = 0; i < processing.pendingEvents.size(); i++) {

                Event e = processing.pendingEvents.get(i);

                if (!sequential.queueEvent(e)) {
                    // Failed to deliver event locally, so dispatch to parent
                    parent.forwardEvent(e);
                }
            }

            processing.pendingEvents.clear();
        }

        if (processing.deliveredEvents.size() > 0) {

            for (int i = 0; i < processing.deliveredEvents.size(); i++) {

                Event e = processing.deliveredEvents.get(i);

                if (!sequential.queueEvent(e)) {
                    // Failed to deliver event locally, so dispatch to parent
                    System.err.println("EEP: Cohort " + identifier
                            + " failed to deliver event: " + e);
                    new Exception().printStackTrace(System.err);
              
                    parent.undeliverableEvent(workerID, e);
                    
                    //System.exit(1);
                }
            }

            processing.deliveredEvents.clear();
        }

        if (processing.pendingCancelations.size() > 0) {

            for (int i = 0; i < processing.pendingCancelations.size(); i++) {
                sequential.cancel(processing.pendingCancelations.get(i));
            }

            processing.pendingCancelations.clear();
        }

        while (processing.stealRequests.size() > 0) {

            StealRequest r = processing.stealRequests.remove(0);
            
            long time = System.currentTimeMillis();
            
            if (time <= r.getTimeout()) { 
                
                ActivityRecord a = sequential.steal(r.context);

                if (a != null) { 
                
                    System.err.println("Worker " + workerID + " sending steal " 
                            + "reply"); 
                      
                    parent.sendStealReply(r, a);
                } else { 
               
                    System.err.println("Worker " + workerID + " returning steal " 
                            + "reply"); 
                    
                    parent.returnStealRequest(workerID, r);
                }
            } else { 
            
                System.err.println("Worker " + workerID + " sending failed " 
                        + " steal reply (timeout)"); 
                
                // Stale request!
                parent.sendStealReply(r);
            }
        }
    }

    public void run() {

        // NOTE: For D&C applications it seems to be most efficient to
        // process a single command (i.e., a submit or an event) and then
        // process all changes that occurred in the activities.

        long start = System.currentTimeMillis();

        while (!getDone()) {

            long t1 = System.currentTimeMillis();

            if (PROFILE && t1 > profileDeadline) {
                printProfileInfo(t1);
                profileDeadline = t1 + profileDelta;
            }

            if (havePendingRequests) { 
                processNextCommands();
            }
                
            long t2 = System.currentTimeMillis();
            
            
            // NOTE: one problem here is that we cannot tell if we did any work 
            // or not. We would like to know, since this allows us to reset
            // several variables (e.g., sleepIndex)
            
            boolean more = sequential.process();

            while (more && !havePendingRequests) {
                more = sequential.process();
            }

            long t3 = System.currentTimeMillis();

            if (!more && !havePendingRequests) {
                
                ActivityRecord tmp = null;
                
                if (currentSteal == null || t3 > currentSteal.getTimeout()) { 
                    // Last steal is answered or has timed out
                    currentSteal = new StealRequest(identifier, getContext());
                    currentSteal.setLocal(true);
                    currentSteal.setTimeout(t3 + 1000);
                        
                    tmp = parent.stealAttempt(workerID, currentSteal);

                    stealCount++;
                } else { 
                    tmp = parent.getStoredActivity(getContext());
                }
               
                if (tmp != null) { 
                    currentSteal = null;
                    sleepIndex= 0;
                    sequential.addActivityRecord(tmp);
                } else { 
                    long t4 = System.currentTimeMillis();

                    try {
                        
                        long sleepTime = SLEEP_TIMES[sleepIndex];
                        
                        if (sleepIndex < SLEEP_TIMES.length-1) { 
                            sleepIndex++;
                        }
                        
                        parent.workerIdle(workerID, sleepTime);
                   
                        Thread.sleep(sleepTime);
                        sleepCount++;
                    } catch (Exception e) {
                        // ignored
                    }

                    parent.workerActive(workerID);
                    
                    
                    long t5 = System.currentTimeMillis();

                    sleepTime += t5 - t4;
                }
            }
                
            commandTime += t2 - t1;
            activeTime += t3 - t2;
        }

        long time = System.currentTimeMillis() - start;

        printStatistics(time);
    }

    private void printProfileInfo(long t) {

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

        final long computationTime = sequential.getComputationTime();
        final long activitiesInvoked = sequential.getActivitiesInvoked();
        final long activitiesSubmitted = sequential.getActivitiesSubmitted();

        final long messagesInternal = sequential.getMessagesInternal();
        final long messagesExternal = sequential.getMessagesExternal();

        final long steals = sequential.getSteals();
        final long stealSuccessIn = sequential.getStealSuccess();

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

            ThreadInfo info = management.getThreadInfo(Thread.currentThread()
                    .getId());

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

            System.out.println("   sleep count: " + sleepCount);
            System.out.println("   sleep time : " + sleepTime + " ms. ("
                    + sleepPerc + " %)");

            if (PROFILE) {

                System.out.println("   cpu time   : " + cpuTime + " ms. ("
                        + cpuPerc + " %)");

                System.out.println("   user time  : " + userTime + " ms. ("
                        + userPerc + " %)");

                System.out.println("   blocked    : " + blocked + " times");

                System.out.println("   block time : " + blockedTime + " ms. ("
                        + blockedPerc + " %)");

                System.out.println("   waited     : " + waited + " times");

                System.out.println("   wait time  : " + waitedTime + " ms. ("
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
            System.out.println("   success in : " + stealSuccessIn);
            System.out.println("   outgoing   : " + stealCount);
            System.out.println("   success out: " + stealSuccess);
            System.out.println("   time       : " + stealTime + " ms. (" + stealPerc + " %)");
        
        }
    }

    public CohortIdentifier identifier() {
        return identifier;
    }

    public boolean isMaster() {
        return parent.isMaster();
    }

    public Context getContext() {
        return sequential.getContext();
    }

    public void setContext(Context context) {
        throw new IllegalStateException("setContext not allowed!");
    }
}
