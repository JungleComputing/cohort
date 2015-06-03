package ibis.constellation.impl;

import ibis.constellation.Activity;
import ibis.constellation.ActivityContext;
import ibis.constellation.ActivityIdentifier;
import ibis.constellation.ActivityIdentifierFactory;
import ibis.constellation.Constellation;
import ibis.constellation.ConstellationIdentifier;
import ibis.constellation.Event;
import ibis.constellation.Executor;
import ibis.constellation.StealPool;
import ibis.constellation.StealStrategy;
import ibis.constellation.WorkerContext;
import ibis.constellation.extra.CircularBuffer;
import ibis.constellation.extra.ConstellationLogger;
import ibis.constellation.extra.Debug;
import ibis.constellation.extra.SmartSortedWorkQueue;
import ibis.constellation.extra.WorkQueue;

import java.util.HashMap;
import java.util.Properties;

public class ExecutorWrapper implements Constellation {

    private static final boolean PROFILE = true;

    private static int QUEUED_JOB_LIMIT = 1000000;

    private final SingleThreadedConstellation parent;

    private final ConstellationIdentifier identifier;

    // private PrintStream out;
    private final ConstellationLogger logger;

    private final Executor executor;

    private final WorkerContext myContext;

    private final StealStrategy localStealStrategy;
    private final StealStrategy constellationStealStrategy;
    private final StealStrategy remoteStealStrategy;

    private HashMap<ActivityIdentifier, ActivityRecord> lookup = new HashMap<ActivityIdentifier, ActivityRecord>();

    private final WorkQueue restricted;
    private final WorkQueue fresh;

    private CircularBuffer runnable = new CircularBuffer(1);
    private CircularBuffer relocated = new CircularBuffer(1);

    private ActivityIdentifierFactory generator;

    private long computationTime;

    private long activitiesSubmitted;
    private long activitiesAdded;

    private long wrongContextSubmitted;
    private long wrongContextAdded;

    private long wrongContextDiscovered;

    private long activitiesInvoked;

    private long steals;
    private long stealSuccess;
    private long stolenJobs;

    private long messagesInternal;
    private long messagesExternal;
    private long messagesTime;

    private ActivityRecord current;

    ExecutorWrapper(SingleThreadedConstellation parent, Executor executor,
            Properties p, ConstellationIdentifier identifier,
            ConstellationLogger logger) throws Exception {
        this.parent = parent;
        this.identifier = identifier;
        this.generator = parent.getActivityIdentifierFactory(identifier);
        this.logger = logger;
        this.executor = executor;

        QUEUED_JOB_LIMIT = Integer.parseInt(p.getProperty(
                "ibis.constellation.queue.limit", "" + QUEUED_JOB_LIMIT));

        System.out.println("Executor set job limit to " + QUEUED_JOB_LIMIT);

        restricted = new SmartSortedWorkQueue("ExecutorWrapper(" + identifier
                + ")-restricted");
        fresh = new SmartSortedWorkQueue("ExecutorWrapper(" + identifier
                + ")-fresh");

        executor.connect(this);
        myContext = executor.getContext();

        localStealStrategy = executor.getLocalStealStrategy();
        constellationStealStrategy = executor.getConstellationStealStrategy();
        remoteStealStrategy = executor.getRemoteStealStrategy();
    }

    public void cancel(ActivityIdentifier id) {

        ActivityRecord ar = lookup.remove(id);

        if (ar == null) {
            return;
        }

        if (ar.needsToRun()) {
            runnable.remove(ar);
        }
    }

    public void done() {
        if (lookup.size() > 0) {
            logger.warn("Quiting Constellation with " + lookup.size()
                    + " activities in queue");
        }
    }

    private ActivityRecord dequeue() {

        // Try to dequeue an activity that we can run.

        // First see if any suspended activities have woken up.
        int size = runnable.size();

        if (size > 0) {
            return (ActivityRecord) runnable.removeFirst();
        }

        // Next see if we have any relocated activities.
        size = relocated.size();

        if (size > 0) {
            return (ActivityRecord) relocated.removeFirst();
        }

        // Next see if there are any activities that cannot
        // leave this constellation
        size = restricted.size();

        if (size > 0) {
            return restricted.steal(myContext, localStealStrategy);
        }

        // Finally, see if there are any fresh activities.
        size = fresh.size();

        if (size > 0) {
            return fresh.steal(myContext, localStealStrategy);
        }

        return null;
    }

    private ActivityIdentifier createActivityID(boolean expectsEvents) {

        try {
            return generator.createActivityID(expectsEvents);
        } catch (Exception e) {
            // Oops, we ran out of IDs. Get some more from our parent!
            if (parent != null) {
                generator = parent.getActivityIdentifierFactory(identifier);
            }
        }

        try {
            return generator.createActivityID(expectsEvents);
        } catch (Exception e) {
            throw new RuntimeException(
                    "INTERNAL ERROR: failed to create new ID block!", e);
        }

        // return new MTIdentifier(nextID++);
    }

    void addPrivateActivity(ActivityRecord a) {
        // add an activity that only I am allowed to run, either because
        // it is relocated, or because we have just obtained it and we don't
        // want anyone else to steal it from us.

        // System.out.println("ExectutorWrapper got private activity " +
        // a.identifier());

        lookup.put(a.identifier(), a);
        relocated.insertLast(a);
    }

    protected ActivityRecord lookup(ActivityIdentifier id) {
        return lookup.get(id);
    }

    public ActivityIdentifier submit(Activity a) {

        activitiesSubmitted++;

        ActivityIdentifier id = createActivityID(a.expectsEvents());
        a.initialize(id);

        ActivityRecord ar = new ActivityRecord(a);
        ActivityContext c = a.getContext();

        /*
         * if (restricted.size() + fresh.size() >= QUEUED_JOB_LIMIT) { // If we
         * have too much work on our hands we push it to out parent. Added bonus
         * // is that others can access it without interrupting me.
         * 
         * System.out.println("Executor SHOULD push work to parent! " +
         * restricted.size() + " " + fresh.size()); }
         */

        if (c.satisfiedBy(myContext, StealStrategy.ANY)) {

            lookup.put(a.identifier(), ar);

            if (ar.isRestrictedToLocal()) {
                restricted.enqueue(ar);
            } else {
                fresh.enqueue(ar);
            }
        } else {
            // TODO: shouldn't we batch these calls.
            parent.deliverWrongContext(ar);
        }

        return id;
    }

    public void send(Event e) {

        long start, end;

        if (PROFILE) {
            start = System.currentTimeMillis();
        }

        if (Debug.DEBUG_EVENTS) {
            logger.info("SEND EVENT " + e.source + " to " + e.target + " at "
                    + start);
        }

        // First check if the activity is local.
        ActivityRecord ar = lookup.get(e.target);

        if (ar != null) {

            messagesInternal++;

            ar.enqueue(e);

            boolean change = ar.setRunnable();

            if (change) {
                runnable.insertLast(ar);
            }

            if (PROFILE) {
                end = System.currentTimeMillis();
                messagesTime += (end - start);
            }

            return;
        }

        // Activity is not local, so let our parent handle it.
        parent.handleEvent(e);

        if (PROFILE) {
            end = System.currentTimeMillis();
            messagesTime += (end - start);
        }
    }

    public boolean queueEvent(Event e) {

        ActivityRecord ar = lookup.get(e.target);

        if (ar != null) {

            ar.enqueue(e);

            boolean change = ar.setRunnable();

            if (change) {
                runnable.insertLast(ar);
            }

            // System.out.println("EW: queued event for " + e.target);

            return true;
        }

        System.out
                .println("ERROR: Cannot deliver event: Failed to find activity "
                        + e.target);
        logger.error("ERROR: Cannot deliver event: Failed to find activity "
                + e.target);

        return false;
    }

    protected ActivityRecord[] steal(WorkerContext context, StealStrategy s,
            boolean allowRestricted, int count, ConstellationIdentifier source) {

        // logger.warn("In STEAL on BASE " + context + " " + count);

        steals++;

        ActivityRecord[] result = new ActivityRecord[count];

        // FIXME: Optimize this!!!
        for (int i = 0; i < count; i++) {
            result[i] = doSteal(context, s, allowRestricted);

            if (result[i] == null) {

                if (i == 0) {
                    return null;
                } else {
                    stolenJobs += i;
                    stealSuccess++;
                    return result;
                }
            }
        }

        // logger.warn("STEAL(" + count + ") only produced ALL results");

        stolenJobs += count;
        stealSuccess++;
        return result;
    }

    private ActivityRecord doSteal(WorkerContext context, StealStrategy s,
            boolean allowRestricted) {

        if (Debug.DEBUG_STEAL) {
            logger.info("STEAL BASE(" + identifier + "): activities F: "
                    + fresh.size() + " W: " + /* wrongContext.size() + */" R: "
                    + runnable.size() + " L: " + lookup.size());
        }

        if (allowRestricted) {

            ActivityRecord r = restricted.steal(context, s);

            if (r != null) {

                // Sanity check -- should not happen!
                if (r.isStolen()) {
                    logger.warn("INTERNAL ERROR: return stolen job "
                            + identifier);
                }

                lookup.remove(r.identifier());

                if (Debug.DEBUG_STEAL) {
                    logger.info("STOLEN " + r.identifier());
                }

                return r;
            }

            // If restricted fails we try the regular queue
        }

        ActivityRecord r = fresh.steal(context, s);

        if (r != null) {

            // Sanity check -- should not happen!
            if (r.isStolen()) {
                logger.error("INTERNAL ERROR: return stolen job " + identifier);
            }

            lookup.remove(r.identifier());

            if (Debug.DEBUG_STEAL) {
                logger.info("STOLEN " + r.identifier());
            }

            return r;
        }

        return null;
    }

    private void process(ActivityRecord tmp) {

        long start, end;

        tmp.activity.setExecutor(executor);

        current = tmp;

        if (PROFILE) {
            start = System.currentTimeMillis();
        }

        tmp.run();

        if (PROFILE) {
            end = System.currentTimeMillis();

            computationTime += end - start;

            activitiesInvoked++;
        }

        if (tmp.needsToRun()) {
            runnable.insertFirst(tmp);
        } else if (tmp.isDone()) {
            cancel(tmp.identifier());
        }

        current = null;
    }

    boolean process() {

        ActivityRecord tmp = dequeue();

        // NOTE: the queue is guaranteed to only contain activities that we can
        // run. Whenever new activities are added or the the context of
        // this cohort changes we filter out all activities that do not
        // match.

        if (tmp != null) {
            process(tmp);
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

    long getActivitiesAdded() {
        return activitiesAdded;
    }

    long getWrongContextSubmitted() {
        return wrongContextSubmitted;
    }

    long getWrongContextAdded() {
        return wrongContextAdded;
    }

    long getWrongContextDiscovered() {
        return wrongContextDiscovered;
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

    long getStolen() {
        return stolenJobs;
    }

    public ConstellationIdentifier identifier() {
        return identifier;
    }

    public boolean isMaster() {
        return parent.isMaster();
    }

    public WorkerContext getContext() {
        return myContext;
    }

    public StealStrategy getLocalStealStrategy() {
        return localStealStrategy;
    }

    public StealStrategy getConstellationStealStrategy() {
        return constellationStealStrategy;
    }

    public StealStrategy getRemoteStealStrategy() {
        return remoteStealStrategy;
    }

    public boolean activate() {
        /*
         * if (parent != null) { return true; }
         * 
         * while (process());
         */
        return false;
    }

    public boolean processActitivies() {
        return parent.processActivities();
    }

    public void runExecutor() {

        try {
            executor.run();
        } catch (Exception e) {
            logger.error("Executor terminated unexpectedly!", e);
        }
    }

    StealPool belongsTo() {
        return executor.belongsTo();
    }

    StealPool stealsFrom() {
        return executor.stealsFrom();
    }
}
