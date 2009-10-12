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
        
        // These are the new submits 
        final ArrayList<Activity> pendingSubmit = new ArrayList<Activity>();
        
        final ArrayList<ActivityRecord> deliveredActivityRecords = new ArrayList<ActivityRecord>();
       
        final ArrayList<Event> pendingEvents = new ArrayList<Event>();

        final ArrayList<Event> deliveredEvents = new ArrayList<Event>();

        final ArrayList<ActivityIdentifier> pendingCancelations = new ArrayList<ActivityIdentifier>();

        final ArrayList<LocalStealRequest> stealRequests = new ArrayList<LocalStealRequest>();
        
        boolean cancelAll = false;
        
        public String print() { 
            return "QUEUES: " + pendingSubmit.size() + " " 
                    + deliveredActivityRecords.size() + " " 
                    + pendingEvents.size() + " " 
                    + deliveredEvents.size() + " "
                    + pendingCancelations.size() + " " + 
                    + stealRequests.size();
        }
    }
    
    private PendingRequests incoming = new PendingRequests();
    private PendingRequests processing = new PendingRequests();

    private boolean done = false;

    private boolean idle = false;
        
    
    
    
    private long eventTime;
    private long activeTime;
    private long idleTime;
    private long idleCount;
    
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

    private boolean havePendingRequests = false;

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

    public synchronized void cancel(ActivityIdentifier id) {

        incoming.pendingCancelations.add(id);
        havePendingRequests = true;
    }
    
    public synchronized void postStealRequest(LocalStealRequest s) {

        incoming.stealRequests.add(s);
        havePendingRequests = true;
    }
    
    public synchronized void deliverEvent(Event e) {

        incoming.deliveredEvents.add(e);
        havePendingRequests = true;
    }
    
    public synchronized void deliverActivityRecord(ActivityRecord a) {

        incoming.deliveredActivityRecords.add(a);
        havePendingRequests = true;
    }

    
    public ActivityIdentifier submit(Activity a) {

        // System.out.println("ST submit");

        ActivityIdentifier id = sequential.prepareSubmission(a);
        
      //  System.out.println("Activity " + id.localName() + " created!");
     
        synchronized (this) {
            incoming.pendingSubmit.add(a);
            havePendingRequests = true;
        }
        
        return id;
    }

    public synchronized void send(ActivityIdentifier source, ActivityIdentifier target,
            Object o) {
        
        incoming.pendingEvents.add(new MessageEvent(source, target, o));
        havePendingRequests = true;
    }

    private synchronized boolean getDone() {
        return done;
    }
  
    public synchronized void done() {
        done = true;
    }

    private synchronized void swapEventQueues() {
        
        if (idle) { 
            System.out.println("Processing events while idle!\n" + incoming.print() + "\n" + processing.print());
        }
        
        PendingRequests tmp = incoming;
        incoming = processing;
        processing = tmp;
        havePendingRequests = false;
    }

    private void processActivityRecords() { 
        if (processing.deliveredActivityRecords.size() > 0) {

            for (int i = 0; i < processing.deliveredActivityRecords.size(); i++) {
                sequential.addActivityRecord(processing.deliveredActivityRecords.get(i));
            }

            processing.deliveredActivityRecords.clear();
        }
    }
    
    private void processSubmits() { 
        if (processing.pendingSubmit.size() > 0) {

            for (int i = 0; i < processing.pendingSubmit.size(); i++) {
                sequential.finishSubmission(processing.pendingSubmit.get(i));
            }

            processing.pendingSubmit.clear();
        }
    }
    
    private void processLocalEvents() { 
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
    }
    
    private void processRemoteEvents() { 
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
    }
    
    private void processCancellations() { 
        if (processing.pendingCancelations.size() > 0) {

            for (int i = 0; i < processing.pendingCancelations.size(); i++) {
                sequential.cancel(processing.pendingCancelations.get(i));
            }

            processing.pendingCancelations.clear();
        }
    }

    private void processStealRequests() { 
        
        while (processing.stealRequests.size() > 0) {

            LocalStealRequest s = processing.stealRequests.remove(0);
            
            ActivityRecord a = sequential.steal(s.context);

            if (a != null) { 
                parent.stealReply(workerID, s, a);
            } else { 
                parent.stealReply(workerID, s, null);
            }
        }
    }
    
    
    private void processEvents() {
       
        swapEventQueues();
        
        processActivityRecords();
        processSubmits();
        processLocalEvents();
        processRemoteEvents();
        processCancellations();
        processStealRequests();
    }

    private synchronized boolean havePendingRequests() {
        return havePendingRequests;
    }
    
    public void run() {

        // NOTE: For D&C applications it seems to be most efficient to
        // process a single command (i.e., a submit or an event) and then
        // process all changes that occurred in the activities.

        long start = System.currentTimeMillis();

        while (!getDone()) {

            long t1 = System.currentTimeMillis();

        //    if (PROFILE && t1 > profileDeadline) {
       //         printProfileInfo(t1);
        //        profileDeadline = t1 + profileDelta;
        //    }

            if (havePendingRequests()) { 
                processEvents();
            }
                
            long t2 = System.currentTimeMillis();
            
            // NOTE: one problem here is that we cannot tell if we did any work 
            // or not. We would like to know, since this allows us to reset
            // several variables (e.g., sleepIndex)
            
            boolean more = sequential.process();

            while (more) {
                more = sequential.process();
           
                // FIXME performance killer!
                more = more && !havePendingRequests();
            }

            long t3 = System.currentTimeMillis();

            while (!more && !havePendingRequests()) {
                
                synchronized (this) {
                    System.out.println(System.currentTimeMillis() 
                            + " IDLE(" + workerID + ") " 
                            + sequential.printState() + " " 
                            + incoming.print() 
                            + " / " + processing.print());
                    idle = true;
                }
                
                more = parent.idle(workerID, getContext());
           
                if (!more) { 
                    
                    if (getDone()) { 
                        break;
                    }
                    
                    try { 
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        // ignored
                    }
                }
            }
    
            // FIXME: remove when debugged!
            synchronized (this) {
                idle = false;
            }
            
            long t4 = System.currentTimeMillis();
                
            eventTime   += t2 - t1;
            activeTime  += t3 - t2;
            idleTime    += t4 - t3;
        }

        long time = System.currentTimeMillis() - start;

        printStatistics(time);
    }

    /*
    private ActivityRecord idle() { 
        
        ActivityRecord tmp = null;
                
        if (currentSteal == null || t3 > currentSteal.getTimeout()) { 
                    
            // Last steal is answered or has timed out
            if (currentSteal != null) { 
                stealTimeout++;
            }
                    
                    currentSteal = new StealRequest(identifier, getContext());
                    currentSteal.setLocal(true);
                    currentSteal.setTimeout(t3 + 1000); // FIXME (use property)
                        
                    tmp = parent.stealAttempt(workerID, currentSteal);

                    stealCount++;
                } else { 
                    tmp = parent.getStoredActivity(getContext());
                }
            
                long t4 = System.currentTimeMillis();

                stealTime += t4 - t3;
                
                if (tmp != null) { 
                    stealSuccess++;
                    
                    currentSteal = null;
                    sleepIndex= 0;
                    sequential.addActivityRecord(tmp);
                } else { 
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
*/
        
   

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

        final double eventPerc = (100.0 * eventTime) / totalTime;
        final double activePerc = (100.0 * activeTime) / totalTime;
        final double idlePerc = (100.0 * idleTime) / totalTime;
        
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

            System.out.println("   command    : " + eventTime + " ms. ("
                    + eventPerc + " %)");

            System.out.println("   idle count: " + idleCount);
            System.out.println("   idle time : " + idleTime + " ms. ("
                    + idlePerc + " %)");

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
