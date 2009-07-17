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
import java.util.HashMap;

class BaseCohort implements Cohort {

    private static final boolean PROFILE = true;

    private final MultiThreadedCohort parent;

    private final CohortIdentifier identifier;

    private final ThreadMXBean management;

    private HashMap<ActivityIdentifier, ActivityRecord> local = 
        new HashMap<ActivityIdentifier, ActivityRecord>();

    private CircularBuffer fresh = new CircularBuffer(16);

    private CircularBuffer runnable = new CircularBuffer(16);

    private DistributedActivityIdentifierGenerator generator;

    private long computationTime;
    
    private long activitiesSubmitted;

    private long activitiesInvoked;

    private long steals;

    private long messagesInternal;

    private long messagesExternal;


    BaseCohort(MultiThreadedCohort parent, CohortIdentifier identifier) {

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
        this.generator = parent.getIDGenerator(identifier);
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
            return (ActivityRecord) runnable.removeLast();
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
    }

    public boolean queueEvent(Event e) {

        ActivityRecord ar = local.get(e.target);

        if (ar == null) {
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

    void steal() {

        steals++;

        int size = fresh.size();

        if (size > 0) {

            // Get the first of the new jobs (this is assumed to be the largest
            // one)
            // remove it from our administration, and hand it over to our
            // parent.
            ActivityRecord r = (ActivityRecord) fresh.removeFirst();
            // System.out.println("STEAL " + size + " " + r.identifier());
            local.remove(r.identifier());
            parent.addActivityRecord(r, true);
        }
    }

    boolean process() {

        ActivityRecord tmp = dequeue();

        if (tmp != null) {

            // System.out.println(workerID + ": Running " + tmp.identifier());

            tmp.activity.setCohort(this);

            long start = System.currentTimeMillis();

            tmp.run();

            computationTime += System.currentTimeMillis() - start;

            activitiesInvoked++;

            if (tmp.needsToRun()) {
                runnable.insertFirst(tmp);
            } else if (tmp.isDone()) {
                cancel(tmp.identifier());
            }

            return true;
        }

        return false;
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
        
        double comp = (100.0 * computationTime) / totalTime;
        double fact = ((double) activitiesInvoked) / activitiesSubmitted;
        
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
            System.out.println("   computation: " + computationTime + " ms. ("
                    + comp + " %)");
            
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
        // TODO Auto-generated method stub
        return null;
    }

    public void setContext(Context context) {
        // TODO Auto-generated method stub

    }
}
