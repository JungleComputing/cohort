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
import ibis.constellation.extra.ActivityLocationLookup;
import ibis.constellation.extra.CircularBuffer;
import ibis.constellation.extra.ConstellationLogger;
import ibis.constellation.extra.Debug;
import ibis.constellation.extra.SmartSortedWorkQueue;
import ibis.constellation.extra.WorkQueue;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

public class SingleThreadedConstellation extends Thread {

    private static final boolean PRINT_ACTIVITY = false;

    private static final boolean PROFILE = true;
    private static final boolean THROTTLE_STEALS = true;
    private static final int DEFAULT_STEAL_DELAY = 50;
    private static final boolean DEFAULT_IGNORE_EMPTY_STEAL_REPLIES = false;

    private final MultiThreadedConstellation parent;

    private final ActivityLocationLookup exportedActivities = new ActivityLocationLookup();
    private final ActivityLocationLookup relocatedActivities = new ActivityLocationLookup();

    private final ExecutorWrapper wrapper;

    // Fresh work that anyone may steal
    private final WorkQueue fresh;

    // Fresh work that can only be stolen by one of my peers
    private final WorkQueue restricted;

    // Work that is stolen from an external constellation. It may be run by me or one of my peers.
    private final WorkQueue stolen;

    // Work that has a context that is not supported by our local executor.
    private final WorkQueue wrongContext;

    // Work that may not leave this machine, but has a context that is not supported by our local executor.
    private final WorkQueue restrictedWrongContext;

    // Work that is relocated. Only our local executor may run it.
    private final CircularBuffer relocated = new CircularBuffer(1);

    // Hashmap allowing quick lookup of the activities in our 4 queues.
    private HashMap<ActivityIdentifier, ActivityRecord> lookup =
        new HashMap<ActivityIdentifier, ActivityRecord>();

    private final ConstellationIdentifier identifier;

    private PrintStream out;
    private ConstellationLogger logger;

    //private final Thread thread;

    private StealPool myPool;
    private StealPool stealPool;

    private int rank;

    private boolean active;

    class Facade implements Constellation {

        @Override
        public ActivityIdentifier submit(Activity a) {
            return performSubmit(a);
        }

        @Override
        public void send(Event e) {
            performSend(e);
        }

        @Override
        public void cancel(ActivityIdentifier aid) {
            performCancel(aid);
        }

        @Override
        public boolean activate() {

            if (parent == null) {
                return performActivate();
            }

            return false;
        }

        @Override
        public void done() {
            if (parent == null) {
                performDone();
            }

            throw new IllegalStateException("Cannot shut down this constellation");
        }

        @Override
        public boolean isMaster() {

            if (parent == null) {
                return true;
            }

            // FIXME: not correct ?
            return false;
        }

        @Override
        public ConstellationIdentifier identifier() {
            return identifier;
        }

        @Override
        public WorkerContext getContext() {
            return wrapper.getContext();
        }
    }

    private static class PendingRequests {

        final ArrayList<EventMessage> deliveredApplicationMessages =
            new ArrayList<EventMessage>();

     //   final ArrayList<ActivityIdentifier> pendingCancelations =
     //       new ArrayList<ActivityIdentifier>();

        final HashMap<ConstellationIdentifier, StealRequest> stealRequests =
            new HashMap<ConstellationIdentifier, StealRequest>();

        public String print() {
            return "QUEUES: " +
                    + deliveredApplicationMessages.size() + " "
                    + stealRequests.size();
        }
    }

    private final int stealSize;
    private final int stealDelay;

    private long nextStealDeadline;

    private PendingRequests incoming = new PendingRequests();
    private PendingRequests processing = new PendingRequests();

    private ActivityIdentifierFactory aidFactory;
    private long startID = 0;
    private long blockSize = 1000000;

    private boolean done = false;

    //private boolean idle = false;

    private long eventTime;
    private long activeTime;
    private long idleTime;
    private long idleCount;

    // NOTE: these are use for performance debugging...
    /*
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
     */

    private final boolean ignoreEmptyStealReplies;

    private volatile boolean havePendingRequests = false;

    SingleThreadedConstellation(Executor executor, Properties p) throws Exception {
        this(null, executor, p);
    }

    public SingleThreadedConstellation(MultiThreadedConstellation parent, Executor executor, Properties p)
        throws Exception {

        super();

       // this.thread = this;
        this.parent = parent;

        if (parent != null) {
            identifier = parent.getCohortIdentifierFactory(null).generateConstellationIdentifier();
            aidFactory = parent.getActivityIdentifierFactory(identifier);
        } else {
            // We're on our own
            identifier = new ConstellationIdentifier(0);
            aidFactory = getActivityIdentifierFactory(identifier);
        }

        stolen = new SmartSortedWorkQueue("ST(" + identifier + ")-stolen");
        restricted = new SmartSortedWorkQueue("ST(" + identifier + ")-restricted");
        fresh = new SmartSortedWorkQueue("ST(" + identifier + ")-fresh");
        wrongContext = new SmartSortedWorkQueue("ST(" + identifier + ")-wrong");
        restrictedWrongContext = new SmartSortedWorkQueue("ST(" + identifier + ")-restrictedwrong");

        super.setName("SingleThreadedConstellation " + identifier.id);

        String outfile = p.getProperty("ibis.constellation.outputfile");

        if (outfile != null) {
            String filename = outfile + "." + identifier.id;

            try {
                out = new PrintStream(new BufferedOutputStream(
                        new FileOutputStream(filename)));
            } catch (Exception e) {
                System.out.println("Failed to open output file " + outfile);
                out = System.out;
            }

        } else {
            out = System.out;
        }

        this.logger = ConstellationLogger.getLogger(SingleThreadedConstellation.class, identifier);

        logger.warn("Starting SingleThreadedConstellation: " + identifier);

        String tmp = p.getProperty("ibis.constellation.steal.delay");

        if (tmp != null && tmp.length() > 0) {
            stealDelay = Integer.parseInt(tmp);
        } else {
            stealDelay = DEFAULT_STEAL_DELAY;
        }

        logger.warn("SingleThreaded: steal delay set to " + stealDelay + " ms.");

        /*
        String tmp = p.getProperty("ibis.cohort.sleep");

        if (tmp != null && tmp.length() > 0) {
            sleepTime = Integer.parseInt(tmp);
        } else {
            sleepTime = 1000;
        }

        logger.warn("SingleThreaded: sleepTime set to " + sleepTime + " ms.");
        */

        tmp = p.getProperty("ibis.constellation.stealsize");

        if (tmp == null) { 
        	tmp = p.getProperty("ibis.constellation.steal.size");
        }
        
        if (tmp != null && tmp.length() > 0) {
            stealSize = Integer.parseInt(tmp);
        } else {
            stealSize = 1;
        }

        logger.warn("SingleThreaded: steal size set to " + stealSize);

        tmp = p.getProperty("ibis.constellation.steal.ignorereplies");

        if (tmp != null && tmp.length() > 0) {
            ignoreEmptyStealReplies = Boolean.parseBoolean(tmp);
        } else {
            ignoreEmptyStealReplies = DEFAULT_IGNORE_EMPTY_STEAL_REPLIES;
        }

        logger.warn("SingleThreaded: ignore empty steal replies set to "
                + ignoreEmptyStealReplies);

        if (PROFILE) {/*
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
            }*/
        }

        wrapper = new ExecutorWrapper(this, executor, p, identifier, logger);

        myPool = wrapper.belongsTo();
        stealPool = wrapper.stealsFrom();

        if (parent != null) {
            parent.register(this);
        }
    }

    void setRank(int rank) {
        this.rank = rank;
    }

    int getRank() {
        return rank;
    }

    StealPool belongsTo() {
        return myPool;
    }

    StealPool stealsFrom() {
        return stealPool;
    }

    WorkerContext getContext() {
        return wrapper.getContext();
    }

    StealStrategy getLocalStealStrategy() {
        return wrapper.getLocalStealStrategy();
    }

    StealStrategy getConstellationStealStrategy() {
        return wrapper.getConstellationStealStrategy();
    }

    StealStrategy getRemoteStealStrategy() {
        return wrapper.getRemoteStealStrategy();
    }

    ConstellationIdentifier identifier() {
        return identifier;
    }

    ActivityIdentifier performSubmit(Activity a) {

        ActivityIdentifier id = createActivityID(a.expectsEvents());
        a.initialize(id);

        ActivityRecord ar = new ActivityRecord(a);
        ActivityContext c = a.getContext();

        if (c.satisfiedBy(wrapper.getContext(), StealStrategy.ANY)) {

            synchronized (this) {
                lookup.put(a.identifier(), ar);

                if (a.isRestrictedToLocal()) {

                    //System.out.println("ST: " + identifier + " sumbit " + id + " to restricted.");

                    restricted.enqueue(ar);
                } else {

                    //System.out.println("ST: " + identifier + " sumbit " + id + " to fresh.");

                    fresh.enqueue(ar);
                }
            }
        } else {
            synchronized (this) {
                lookup.put(ar.identifier(), ar);

                if (a.isRestrictedToLocal()) {
                    restrictedWrongContext.enqueue(ar);
                } else {
                    wrongContext.enqueue(ar);
                }
            }
        }

        return id;
    }

    void performSend(Event e) {
        logger.error("INTERNAL ERROR: Send not implemented!");
    }

    void performCancel(ActivityIdentifier aid) {
        logger.error("INTERNAL ERROR: Cancel not implemented!");
    }

    boolean performActivate() {

        synchronized (this) {
            if (active) {
                return false;
            }

            active = true;
        }

        start();
        return true;
    }

    synchronized void performDone() {

        if (!active) {
            return;
        }

        done = true;
        havePendingRequests = true;
    }

    //ActivityIdentifier deliverSubmit(Activity a) {
     //   return submit(a);
    //}

    private ActivityRecord [] trim(ActivityRecord [] a, int count) {
        ActivityRecord [] result = new ActivityRecord[count];
        System.arraycopy(a, 0, result, 0, count);
        return result;
    }

    ActivityRecord [] attemptSteal(WorkerContext context, StealStrategy s, StealPool pool, ConstellationIdentifier src, int size, boolean local) {

        ActivityRecord [] result = new ActivityRecord[size];

        int count = attemptSteal(result, context, s, pool, src, size, local);

        if (count == 0) {
            return null;
        }

        return trim(result, count);
    }

    synchronized int attemptSteal(ActivityRecord [] tmp, WorkerContext context,
            StealStrategy s, StealPool pool, ConstellationIdentifier src,
            int size, boolean local) {

        // attempted steal request from parent. Expects an immediate reply

        // sanity check
        if (src.equals(identifier)) {
            System.out.println("INTERAL ERROR: attemp steal from self!");
            new Exception().printStackTrace(System.out);
            return 0;
        }

        if (!pool.overlap(wrapper.belongsTo())) {
            return 0;
        }

        // First steal from the activities that I cannot run myself.
        int offset = wrongContext.steal(context, s, tmp, 0, size);

        if (local) {

            // Only peers from our own constellation are allowed to steal restricted or stolen jobs.
            if (offset < size) {
                offset += restrictedWrongContext.steal(context, s, tmp, offset, size-offset);
            }

            if (offset < size) {
                offset += restricted.steal(context, s, tmp, offset, size-offset);
            }

            if (offset < size) {
                offset += stolen.steal(context, s, tmp, offset, size-offset);
            }
        }

        // Anyone may steal a fresh job
        if (offset < size) {
            offset += fresh.steal(context, s, tmp, offset, size-offset);
        }

        if (offset == 0) {
            // steal failed, no activities stolen
            return 0;
        }

        // Success. Trim if necessary
        if (offset != size) {
            tmp = trim(tmp, offset);
        }

        //System.out.println("ST: " + identifier + " returning " + offset + " stolen jobs to " + src);

        // Next, remote activities from lookup, and mark and register them as relocated or stolen/exported
        registerLeavingActivities(tmp, offset, src, local);

        return offset;
    }

    private synchronized void registerLeavingActivities(ActivityRecord [] ar, int len, ConstellationIdentifier dest, boolean isLocal) {

        for (int i=0;i<len;i++) {
            if (ar[i] != null) {
                lookup.remove(ar[i].identifier());

                if (isLocal) {
                    ar[i].setRelocated(true);
                    relocatedActivities.add(ar[i].identifier(), dest);
                } else {
                    ar[i].setStolen(true);
                    exportedActivities.add(ar[i].identifier(), dest);
                }
            }
        }
    }

    void deliverStealRequest(StealRequest sr) {
        // steal request (possibly remote) to enqueue and handle later
        logger.info("S REMOTE STEAL REQUEST from " + sr.source
                + " context " + sr.context);

        postStealRequest(sr);
    }

    private synchronized boolean pushWorkToExecutor(StealStrategy s) {

        //System.out.println("ST: " + identifier + " Pushing work to executor");

        // Push all relocated activities to our executor.
        if (relocated.size() > 0) {
            while (relocated.size() > 0) {
                ActivityRecord ar = (ActivityRecord) relocated.removeFirst();
                lookup.remove(ar.identifier());
                wrapper.addPrivateActivity(ar);
            }

            //System.out.println("ST: " + identifier + " Pushed from relocated");
            return true;
        }

        // Else: push one restricted activity to our executor
        if (restricted.size() > 0) {
            ActivityRecord ar = restricted.steal(wrapper.getContext(), s);
            if (ar != null) {
                lookup.remove(ar.identifier());
                wrapper.addPrivateActivity(ar);
                //System.out.println("ST: " + identifier + " Pushed from restricted");
                return true;
            }
        }

        // Else: push one stolen activity to our executor
        if (stolen.size() > 0) {
            ActivityRecord ar = stolen.steal(wrapper.getContext(), s);
            if (ar != null) {
                lookup.remove(ar.identifier());
                wrapper.addPrivateActivity(ar);
                //System.out.println("ST: " + identifier + " Pushed from stolen");
                return true;
            }
        }

        // Else: push one fresh activity to our executor
        if (fresh.size() > 0) {
            ActivityRecord ar = fresh.steal(wrapper.getContext(), s);
            if (ar != null) {
                lookup.remove(ar.identifier());
                wrapper.addPrivateActivity(ar);
               // System.out.println("ST: " + identifier + " Pushed from fresh");
                return true;
            }
        }

      //  System.out.println("ST: " + identifier + " Failed to push any work " + relocated.size() + " " + restricted.size() + " " + stolen.size() + " " + fresh.size() + " " + lookup.size());
        return false;
    }

    void deliverStealReply(StealReply sr) {

        if (sr.isEmpty()) {
            // ignore empty replies
            return;
        }

        // If we get a non-empty steal reply, we simply enqueue it locally.
        ActivityRecord [] tmp = sr.getWork();

        synchronized (this) {

            for (int i=0;i<tmp.length;i++) {
                ActivityRecord a = tmp[i];

                if (a != null) {
                    // two options here: either the job is stolen (from a remote constellation) or
                    // relocated (from a peer in our local constellation). Stolen jobs may be
                    // relocated later, but relocated jobs must be executed by this executor.

                    //Timo: Add it to lookup as well!
                    lookup.put(a.identifier(), a);
                    if (a.isRelocated()) {
                        relocated.insertLast(a);
                    } else {
                        stolen.enqueue(a);
                    }
                }
            }
        }
    }

    synchronized ConstellationIdentifier deliverEventMessage(EventMessage m) {
        // A message from above. The target must be local (in one of my queues,
        // or in the queues of the executor) or its new location must be known
        // locally.
        //
        // POSSIBLE RACE CONDITIONS:
        //
        // 1) When the message overtakes a steal request, it may arrive before
        //    the activity has arrived. As a result, the activity cannot be found
        //    here yet.
        //
        // 2) The activity may be registered in relocated or exported but
        //    a) it may not have arrived at the destination yet
        //    b) it may about to be reclaimed because the target could not be reached
        //

        // The target activity may be in one of my local queues

        //System.out.println("ST: " + identifier + " Delivering message from " + m.source + " to " + m.event.target);

        Event e = m.event;

        ActivityRecord tmp = lookup.get(e.target);

        if (tmp != null) {
            tmp.enqueue(e);
        //    System.out.println("ST: " + identifier + " success");
            return null;
        }

        // If not, it may have been relocated
        ConstellationIdentifier cid = relocatedActivities.lookup(e.target);

        if (cid != null) {
        //    System.out.println("ST: " + identifier + " relocated to " + cid);
            return cid;
        }

        // If not, it may have been stolen
        cid = exportedActivities.lookup(e.target);

        if (cid != null) {
        //    System.out.println("ST: " + identifier + " exported to " + cid);
            return cid;
        }

        // If not, is should be in the queue of my executor
    //System.out.println("ST: " + identifier + " posted");
        postEventMessage(m);
        return null;
    }

    ActivityIdentifierFactory getActivityIdentifierFactory(ConstellationIdentifier cid) {

        if (parent == null) {
            synchronized (this) {
                ActivityIdentifierFactory tmp = new ActivityIdentifierFactory(cid.id,  startID, startID+blockSize);
                startID += blockSize;
                return tmp;
            }
        }

        return parent.getActivityIdentifierFactory(cid);
    }

    boolean isMaster() {

        if (parent == null) {
            return true;
        }

        // FIXME: this is not correct ??
        return false;
    }

    void handleEvent(Event e) {
        // An event pushed up by our executor. We now the
        // executor itself does not contain the target activity

        ConstellationIdentifier cid = null;

        synchronized (this) {

            // See if the activity is in one of our queues
            ActivityRecord tmp = lookup.get(e.target);

            if (tmp != null) {
                // It is, so enqueue it and return.
                tmp.enqueue(e);
                return;
            }

            // See if we have exported it somewhere
            cid = exportedActivities.lookup(e.target);

            if (cid == null) {
                // If not, it may have been relocated
                cid = relocatedActivities.lookup(e.target);
            }

            if (cid == null) {
                // If not, we simply send the event to the parent
                cid = e.target.getOrigin();
            }
        }

        if (cid.equals(identifier)) {
            // the target is local, which means we have lost a local activity
            logger.error("ERROR: activity " + e.target + " does no longer exist! (event dropped)");
            return;
        }

        parent.handleEventMessage(new EventMessage(identifier, cid, e));
    }

    private final void signal() {
        havePendingRequests = true;
        //thread.interrupt();
    }

    private void postStealRequest(StealRequest s) {

        // sanity check
        if (s.source.equals(identifier)) {
            System.out.println("INTERAL ERROR: posted steal request from self!");
            new Exception().printStackTrace(System.out);
            return;
        }

        synchronized (incoming) {

            if (Debug.DEBUG_STEAL) {
                StealRequest tmp = incoming.stealRequests.get(s.source);

                if (tmp != null) {
                    logger.info("Steal request overtaken: " + s.source);
                    //System.out.println("Steal request overtaken: " + s.source);
                }
            }

            // System.out.println("Steal request queued: " + s.source);
            incoming.stealRequests.put(s.source, s);
        }
        //havePendingRequests = true;
        signal();
    }

    private void postEventMessage(EventMessage m) {
        synchronized (incoming) {
            incoming.deliveredApplicationMessages.add(m);
        }
        //havePendingRequests = true;
        signal();
    }

   private synchronized ActivityIdentifier createActivityID(boolean expectsEvents) {

        try {
            return aidFactory.createActivityID(expectsEvents);
        } catch (Exception e) {
            // Oops, we ran out of IDs. Get some more from our parent!
            if (parent != null) {
                aidFactory = parent.getActivityIdentifierFactory(identifier);
            } else {
                aidFactory = getActivityIdentifierFactory(identifier);
            }
        }

        try {
            return aidFactory.createActivityID(expectsEvents);
        } catch (Exception e) {
            throw new RuntimeException(
                    "INTERNAL ERROR: failed to create new ID block!", e);
        }
    }

    private synchronized boolean getDone() {
        return done;
    }

    private void swapEventQueues() {

        if (Debug.DEBUG_SUBMIT) {
            logger.info("Processing events while idle!\n" + incoming.print()
                    + "\n" + processing.print());
        }

        synchronized (incoming) {
            PendingRequests tmp = incoming;
            incoming = processing;
            processing = tmp;
            // NOTE: havePendingRequests needs to be set here to prevent a gap
            // between doing the swap + setting it to false. Another submit
            // could potentially use this gap to insert a new event. This would
            // lead to a race condition!
            havePendingRequests = false;
        }
    }

    private void processRemoteMessages() {

        if (processing.deliveredApplicationMessages.size() > 0) {

            for (int i = 0; i < processing.deliveredApplicationMessages.size(); i++) {

                EventMessage m = processing.deliveredApplicationMessages.get(i);

                if (!wrapper.queueEvent(m.event)) {
                    // Failed to deliver event locally. Check if the activity is now in
                    // one of the local queues. If not, return to parent.

                    logger.warn("WARNING: Failed to deliver message from "
                            + m.source + " / " + m.event.source + " to "
                            + m.target + " / " + m.event.target + " (resending)");
                    //logger.warn("message contents: " + m.event.toString());

                    handleEvent(m.event);
                }
            }

            processing.deliveredApplicationMessages.clear();
        }
    }

/*
    private void processCancellations() {
        if (processing.pendingCancelations.size() > 0) {

            for (int i = 0; i < processing.pendingCancelations.size(); i++) {
                wrapper.cancel(processing.pendingCancelations.get(i));
            }

            processing.pendingCancelations.clear();
        }
    }
*/
    void reclaim(ActivityRecord [] a) {

        if (a == null) {
            return;
        }

        for (int i=0;i<a.length;i++) {

            ActivityRecord ar = a[i];
            ActivityContext c = ar.getContext();

            if (ar.isRelocated()) {
                // We should unset the relocation flag if an activity is returned.
                ar.setRelocated(false);
                relocated.remove(ar.identifier());
            } else if (ar.isStolen()) {

                // Sanity check -- should never fire! FIXME --remove!
                if (!ar.identifier().getOrigin().equals(identifier)) {
                    System.out.println("INTERNAL ERROR: resetting stolen  ");
                }

                // We should unset the stolen flag if an activity is returned.
                ar.setStolen(false);
                exportedActivities.remove(ar.identifier());
            }

            if (c.satisfiedBy(wrapper.getContext(), StealStrategy.ANY)) {

                synchronized (this) {
                    lookup.put(ar.identifier(), ar);

                    if (ar.isRestrictedToLocal()) {
                        restricted.enqueue(ar);
                    } else if (ar.isStolen()) {
                        stolen.enqueue(ar);
                    } else {
                        fresh.enqueue(ar);
                    }
                }
            } else {
                synchronized (this) {
                    lookup.put(ar.identifier(), ar);

                    if (ar.isRestrictedToLocal()) {
                        restrictedWrongContext.enqueue(ar);
                    } else {
                        wrongContext.enqueue(ar);
                    }
                }
            }
        }
    }

    private void processStealRequests() {

        if (processing.stealRequests.size() == 0) {
            return;
        }

        for (StealRequest s : processing.stealRequests.values()) {

            // Make sure the steal request is still valid!
            if (!s.getStale()) {

                ActivityRecord [] a = null;

                synchronized (this) {
                    // We grab the lock here to prevent other threads (from above) from doing a
                    // lookup in the relocated/exported tables while we are removing activities
                    // from the executor's queue.

                    StealStrategy tmp = s.isLocal() ? s.constellationStrategy : s.remoteStrategy;

                    // NOTE: a is allowed to be null
                    a = wrapper.steal(s.context, tmp, s.isLocal(), s.size, s.source);

                    if (a != null) {
                        // We have a result. Register the leaving activities.
                        registerLeavingActivities(a, a.length, s.source, s.isLocal());
                    }
                }

                if (a != null) {
                    if (!parent.handleStealReply(this, new StealReply(wrapper.identifier(), s.source, s.pool, s.context, a))) {
                        reclaim(a);
                    }
                } else if (!ignoreEmptyStealReplies) {
                    // No result, but we send a reply anyway.
                    parent.handleStealReply(this, new StealReply(wrapper.identifier(), s.source, s.pool, s.context, a));
                } else {
                    // No result, and we're not supposed to tell anyone
                    if (Debug.DEBUG_STEAL) {
                        logger.info("IGNORING empty steal reply");
                    }
                }

            } else {
                if (Debug.DEBUG_STEAL) {
                    logger.info("DROPPING STALE STEAL REQUEST");
                }
            }
        }

        processing.stealRequests.clear();
    }

    private void processEvents() {

        swapEventQueues();

        processRemoteMessages();
       //  processCancellations();
        processStealRequests();
    }

    private boolean pauseUntil(long deadline) {

        long pauseTime = deadline - System.currentTimeMillis();

        if (pauseTime > 0) {

            boolean wake = havePendingRequests;

            while (!wake) {

                try {
                    long tmp = System.currentTimeMillis();
                    Thread.sleep(pauseTime);

                    tmp = System.currentTimeMillis() - tmp;

                  //  System.out.println("Sleep took " + tmp);
                } catch (Exception e) {
                    // ignored
                }

                wake = havePendingRequests;

                if (!wake) {
                    pauseTime = deadline - System.currentTimeMillis();
                    wake = (pauseTime <= 0);
                }
            }
        }

        return havePendingRequests;
    }

    private long stealAllowed() {

       if (THROTTLE_STEALS) {

           long now = System.currentTimeMillis();

           if (now >= nextStealDeadline) {
               nextStealDeadline = now + stealDelay;
               return 0;
           }

           return nextStealDeadline;
       } else {
           return 0;
       }
    }

    private void resetStealDeadline() {
        if (THROTTLE_STEALS) {
            nextStealDeadline = 0;
        }
    }

    void deliverWrongContext(ActivityRecord a) {
        //Timo: we should add it to the lookup as well
        lookup.put(a.identifier(), a);

        if (a.isRestrictedToLocal()) {
            restrictedWrongContext.enqueue(a);
        } else {
            wrongContext.enqueue(a);
        }
    }

    private long start;
    private long idlestart;

    boolean processActivities() {

        long t1 = System.currentTimeMillis();

        if (havePendingRequests) {
            processEvents();
        }

        long t2 = System.currentTimeMillis();

        // NOTE: one problem here is that we cannot tell if we did any work
        // or not. We would like to know, since this allows us to reset
        // several variables (e.g., sleepIndex)

        int jobs = 0;

        boolean more = wrapper.process();

        if (more) {
            jobs++;
        }

        while (more && !havePendingRequests) {
            more = wrapper.process();

            if (more) {
                jobs++;
            }
        }

        long t3 = System.currentTimeMillis();

        if (PRINT_ACTIVITY && jobs > 0) {
            if (t2-idlestart > 0) {
                out.println("IDLE from " + (idlestart-start) + " to " + (t2-start) + " total " + (t2-idlestart));
            }

            idlestart = t3;

            out.println("ACTIVE from " + (t2-start) + " to "
                    + (t3-start) + " total " + (t3-t2) + " jobs " + jobs);

            out.flush();
        }

        while (!more && !havePendingRequests) {

            // Our executor has run out of work. See if we can find some.

            // Check if there is any matching work in one of the local queues...
            more = pushWorkToExecutor(wrapper.getLocalStealStrategy());

            // If no work was found we send a steal request to our parent.
            if (!more) {

                long nextDeadline = stealAllowed();

                if (nextDeadline == 0) {

                    if (Debug.DEBUG_STEAL) {
                        logger.info("GENERATING STEAL REQUEST at " + identifier
                                + " with context " + getContext());
                    }

                    ActivityRecord [] result = parent.handleStealRequest(this, stealSize);

                    if (result != null) {

                        for (int i=0;i<result.length;i++) {
                            if (result[i] != null) {
                                wrapper.addPrivateActivity(result[i]);
                                more = true;
                            }
                        }

                        if (more) {
                            // ignore steal deadline when we are succesfull!
                            resetStealDeadline();
                        }
                    }
                } else {
                    more = pauseUntil(nextDeadline);
                }
            }
        }

        long t4 = System.currentTimeMillis();

        eventTime   += t2 - t1;
        activeTime  += t3 - t2;
        idleTime    += t4 - t3;

        return getDone();
    }

    public void run() {

        start = System.currentTimeMillis();
        idlestart = start;

        wrapper.runExecutor();

        long time = System.currentTimeMillis() - start;

        printStatistics(time);
    }

    private void printProfileInfo(long t) {
/*
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
        */
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

        final long messagesInternal = wrapper.getMessagesInternal();
        final long messagesExternal = wrapper.getMessagesExternal();
        final long messagesTime = wrapper.getMessagesTime();

        final long computationTime = wrapper.getComputationTime() - messagesTime;
        final long activitiesInvoked = wrapper.getActivitiesInvoked();

        final long activitiesSubmitted = wrapper.getActivitiesSubmitted();
        final long activitiesAdded = wrapper.getActivitiesAdded();

        final long wrongContextSubmitted = wrapper.getWrongContextSubmitted();
        final long wrongContextAdded = wrapper.getWrongContextAdded();
        final long wrongContextDiscovered = wrapper.getWrongContextDiscovered();

        final long steals = wrapper.getSteals();
        final long stealSuccessIn = wrapper.getStealSuccess();
        final long stolen = wrapper.getStolen();

        final double comp = (100.0 * computationTime) / totalTime;
        final double fact = ((double) activitiesInvoked) / (activitiesSubmitted + activitiesAdded);

        final double eventPerc = (100.0 * eventTime) / totalTime;
        final double activePerc = (100.0 * activeTime) / totalTime;
        final double idlePerc = (100.0 * idleTime) / totalTime;
        final double messPerc = (100.0 * messagesTime) / totalTime;

        if (PROFILE) {
            // Get the cpu/user time (in nanos)
            /*
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
            */
        }

        synchronized (out) {

            out.println(identifier + " statistics");
            out.println(" Time");
            out.println("   total      : " + totalTime + " ms.");
            out.println("   active     : " + activeTime + " ms. ("
                    + activePerc + " %)");
            out.println("        run() : " + computationTime + " ms. ("
                    + comp + " %)");

            out.println("   command    : " + eventTime + " ms. ("
                    + eventPerc + " %)");

            out.println("   idle count: " + idleCount);
            out.println("   idle time : " + idleTime + " ms. ("
                    + idlePerc + " %)");

            out.println("   mess time : " + messagesTime + " ms. ("
                    + messPerc + " %)");

            if (PROFILE) {

                out.println("   cpu time   : " + cpuTime + " ms. ("
                        + cpuPerc + " %)");

                out.println("   user time  : " + userTime + " ms. ("
                        + userPerc + " %)");

                out.println("   blocked    : " + blocked + " times");

                out.println("   block time : " + blockedTime + " ms. ("
                        + blockedPerc + " %)");

                out.println("   waited     : " + waited + " times");

                out.println("   wait time  : " + waitedTime + " ms. ("
                        + waitedPerc + " %)");

            }

            out.println(" Activities");
            out.println("   submitted  : " + activitiesSubmitted);
            out.println("   added      : " + activitiesAdded);
            out.println("   invoked    : " + activitiesInvoked + " ("
                    + fact + " /act)");
            out.println("  Wrong Context");
            out.println("   submitted  : " + wrongContextSubmitted);
            out.println("   added      : " + wrongContextAdded);
            out.println("   discovered : " + wrongContextDiscovered);
            out.println(" Messages");
            out.println("   internal   : " + messagesInternal);
            out.println("   external   : " + messagesExternal);
            out.println(" Steals");
            out.println("   incoming   : " + steals);
            out.println("   success    : " + stealSuccessIn);
            out.println("   stolen     : " + stolen);
        }

        out.flush();
    }


}




