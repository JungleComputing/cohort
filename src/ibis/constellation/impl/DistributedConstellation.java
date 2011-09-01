package ibis.constellation.impl;

import ibis.constellation.Activity;
import ibis.constellation.ActivityIdentifier;
import ibis.constellation.Constellation;
import ibis.constellation.ConstellationIdentifier;
import ibis.constellation.Event;
import ibis.constellation.StealPool;
import ibis.constellation.WorkerContext;
import ibis.constellation.context.UnitWorkerContext;
import ibis.constellation.extra.ConstellationIdentifierFactory;
import ibis.constellation.extra.ConstellationLogger;
import ibis.constellation.extra.Debug;

import java.io.PrintStream;
import java.util.Properties;

public class DistributedConstellation {

    private static final int STEAL_POOL   = 1;
    private static final int STEAL_MASTER = 2;
    private static final int STEAL_NONE   = 3;

    private static final boolean PROFILE = true;

    private static boolean REMOTE_STEAL_THROTTLE = true;

    // FIXME setting this to low at startup causes load imbalance!
    //    machines keep hammering the master for work, and (after a while)
    //    get a flood of replies.
    private static long REMOTE_STEAL_TIMEOUT = 1000;

    private static boolean PUSHDOWN_SUBMITS = false;

    private boolean active;

    private MultiThreadedConstellation subConstellation;

    private final ConstellationIdentifier identifier;

    private final Pool pool;

    private final DistributedConstellationIdentifierFactory cidFactory;

    private final ConstellationLogger logger;

    private final DeliveryThread delivery;

    private WorkerContext myContext;

    private long stealReplyDeadLine;

    private boolean pendingSteal = false;

    private final int stealing;

    private final long start;


    private final Facade facade = new Facade();

    private class Facade implements Constellation {

        /* Following methods implement the Constellation interface */

        @Override
        public ActivityIdentifier submit(Activity a) {
            return performSubmit(a);
        }

        @Override
        public void send(Event e) {

            if (!e.target.expectsEvents) {
                throw new IllegalArgumentException("Target activity " + e.target + "  does not expect an event!");
            }

            // An external application wishes to send an event to 'e.target'.
            performSend(e);
        }

        @Override
        public void cancel(ActivityIdentifier aid) {
            // ignored!

            //performCancel(aid);
            //send(new CancelEvent(aid));
        }

        @Override
        public boolean activate() {
            return performActivate();
        }

        @Override
        public void done() {
            performDone();
        }

        @Override
        public boolean isMaster() {
            return pool.isMaster();
        }

        @Override
        public ConstellationIdentifier identifier() {
            return identifier;
        }

        @Override
        public WorkerContext getContext() {
            return handleGetContext();
        }
    }

    public DistributedConstellation(Properties p) throws Exception {

        String tmp = p.getProperty("ibis.cohort.remotesteal.throttle");

        if (tmp != null) {

            try {
                REMOTE_STEAL_THROTTLE = Boolean.parseBoolean(tmp);
            } catch (Exception e) {
                System.err.println("Failed to parse " +
                        "ibis.cohort.remotesteal.throttle: " + tmp);
            }
        }

        tmp = p.getProperty("ibis.cohort.remotesteal.timeout");

        if (tmp != null) {

            try {
                REMOTE_STEAL_TIMEOUT = Long.parseLong(tmp);
            } catch (Exception e) {
                System.err.println("Failed to parse " +
                        "ibis.cohort.remotesteal.timeout: " + tmp);
            }
        }

        tmp = p.getProperty("ibis.cohort.submit.pushdown");

        if (tmp != null) {

            try {
                PUSHDOWN_SUBMITS = Boolean.parseBoolean(tmp);
            } catch (Exception e) {
                System.err.println("Failed to parse " +
                        "ibis.cohort.submits.pushdown: " + tmp);
            }
        }

        String stealName = p.getProperty("ibis.cohort.stealing", "pool");

        if (stealName.equalsIgnoreCase("mw")) {
            stealing = STEAL_MASTER;
        } else if (stealName.equalsIgnoreCase("none")) {
            stealing = STEAL_NONE;
        } else if (stealName.equalsIgnoreCase("pool")) {
            stealing = STEAL_POOL;
        } else {
            System.err.println("Unknown stealing strategy: " + stealName);
            throw new Exception("Unknown stealing strategy: " + stealName);
        }

        myContext = UnitWorkerContext.DEFAULT;

        // Init communication here...
        pool = new Pool(this, p);

        cidFactory = pool.getCIDFactory();
        identifier = cidFactory.generateConstellationIdentifier();

        logger = ConstellationLogger.getLogger(DistributedConstellation.class, identifier);

        delivery = new DeliveryThread(this);
        delivery.start();

        start = System.currentTimeMillis();

        if (true) {
            System.out.println("DistributeConstellation : " + identifier.id);
            System.out.println("               throttle : " + REMOTE_STEAL_THROTTLE);
            System.out.println("         throttle delay : " + REMOTE_STEAL_TIMEOUT);
            System.out.println("               pushdown : " + PUSHDOWN_SUBMITS);
            System.out.println("               stealing : " + stealName);
            System.out.println("                  start : " + start);

        }

        logger.warn("Starting DistributedConstellation " + identifier + " / " + myContext);
    }

    private boolean performActivate() {

        synchronized (this) {
            active = true;
        }

        pool.activate();
        return subConstellation.activate();
    }

    private void performDone() {
        try {
            // NOTE: this will proceed directly on the master. On other
            // instances, it blocks until the master terminates.
            pool.terminate();
        } catch (Exception e) {
            logger.warn("Failed to terminate pool!", e);
        }

        subConstellation.done();

        printStatistics();

        pool.cleanup();
    }

    private synchronized WorkerContext handleGetContext() {
        return myContext;
    }

    private void printStatistics() {

        synchronized (System.out) {


            /*
            System.out.println("Messages send     : " + messagesSend);
            System.out.println("           Events : " + eventsSend);
            System.out.println("           Steals : " + stealsSend);
            System.out.println("             Work : " + workSend);
            System.out.println("          No work : " + no_workSend);
            System.out.println("Messages received : " + messagesReceived);
            System.out.println("           Events : " + eventsReceived);
            System.out.println("           Steals : " + stealsReceived);
            System.out.println("             Work : " + workReceived);
            System.out.println("          No work : " + no_workReceived);
             */
            if (PROFILE) {
                /*
                System.out.println("GC beans     : " + gcbeans.size());

                for (GarbageCollectorMXBean gc : gcbeans) {
                    System.out.println(" GC bean : " + gc.getName());
                    System.out.println("   count : " + gc.getCollectionCount());
                    System.out.println("   time  : " + gc.getCollectionTime());
                }
                 */
            }
        }
    }

    private synchronized boolean setPendingSteal(StealPool pool,
            WorkerContext context, boolean value) {

        // TODO: NEW implementation:
        //
        // Per non-set StealPool we check for each of the contexts if a steal
        // request is pending. If one of the context is not pending yet,
        // we record the steal for all context and allow the request.
        //
        //
        // NOTE: old implementation below!



        // When we are setting the value to false, we don't care about
        // the deadline.
        if (!value) {
            boolean tmp = pendingSteal;
            pendingSteal = false;
            stealReplyDeadLine = 0;
            return tmp;
        }

        long time = System.currentTimeMillis();

        // When we are changing the value from false to true, we also
        // need to set the deadline.
        if (!pendingSteal) {
            pendingSteal = true;
            stealReplyDeadLine = time + REMOTE_STEAL_TIMEOUT;
            return false;
        }

        // When the old value was true but the deadline has passed, we act as
        // if the value was false to begin with
        if (time > stealReplyDeadLine) {
            pendingSteal = true;
            stealReplyDeadLine = time + REMOTE_STEAL_TIMEOUT;
            return false;
        }

        // Otherwise, we leave the value and deadline unchanged
        return true;
    }

    ConstellationIdentifier identifier() {
        return identifier;
    }

    public Constellation getConstellation() {
        return facade;
    }

    PrintStream getOutput() {
        return System.out;
    }

    ActivityIdentifier performSubmit(Activity a) {
        return subConstellation.performSubmit(a);
    }

    void performSend(Event e) {
        subConstellation.performSend(e);
    }

    void performCancel(ActivityIdentifier aid) {
        logger.error("INTERNAL ERROR: cancel not implemented!");
    }

    void deliverRemoteStealRequest(StealRequest sr) {
        // Steal request from network
        //
        // This method is called from an finished upcall. Therefore it
        // may block for a long period of time or communicate.

        if (Debug.DEBUG_STEAL) {
            logger.info("D REMOTE STEAL REQUEST from cohort " + sr.source
                    + " context " + sr.context);
        }

        subConstellation.deliverStealRequest(sr);
    }

    void deliverRemoteStealReply(StealReply sr) {
        // StealReply from network.
        //
        // This method is called from an unfinished upcall. It may NOT
        // block for a long period of time or communicate!

        setPendingSteal(sr.getPool(), sr.getContex(), false);

        if (sr.isEmpty()) {
            // ignore empty steal requests.
            return;
        }

        subConstellation.deliverStealReply(sr);
    }

    void deliverRemoteEvent(EventMessage re) {
        // Event from network.
        //
        // This method is called from an finished upcall. Therefore it
        // may block for a long period of time or communicate.
        subConstellation.deliverEventMessage(re);
    }

    void handleStealRequest(StealRequest sr) {
        // steal request from below
        // FIXME: ADD POOL AND CONTEXT AWARE THROTTLING!!!!

        // A steal request coming in from the subcohort below.

        if (stealing == STEAL_NONE) {
            logger.debug("D STEAL REQUEST swizzled from " + sr.source);
            return;
        }

        if (stealing == STEAL_MASTER && pool.isMaster()) {
            // Master does not steal from itself!
            return;
        }

        if (stealing == STEAL_POOL && (sr.pool == null || sr.pool.isNone())) {
            // Stealing from nobody is easy!
            return;
        }

        StealPool sp = pool.randomlySelectPool(sr.pool);

        if (REMOTE_STEAL_THROTTLE) {

            boolean pending = setPendingSteal(sp, sr.context, true);

            if (pending) {
                // We have already send out a steal in this slot, so
                // we're not allowed to send another one.
                return;
            }
        }

        if (stealing == STEAL_MASTER && pool.forwardToMaster(sr)) {

            if (Debug.DEBUG_STEAL) {
                logger.info("D MASTER FORWARD steal request from child "
                        + sr.source);
            }

        } else if (stealing == STEAL_POOL && pool.randomForwardToPool(sp, sr)) {

            if (Debug.DEBUG_STEAL) {
                logger.info("D RANDOM FORWARD steal request from child "
                        + sr.source  + " to POOL " + sr.pool.getTag());
            }

        }

        logger.fixme("D STEAL REQUEST unknown stealing strategy " + stealing);
    }

    boolean handleApplicationMessage(EventMessage m, boolean enqueueOnFail) {

        // This is triggered as a result of someone in our constellation sending
        // a message (bottom up) or as a result of a incoming remote message
        // being forwarded to some other constellation (when an activity is exported).

        ConstellationIdentifier target = m.target;

        // Sanity check
        if (cidFactory.isLocal(target)) {
            logger.error("INTERNAL ERROR: received message for local constellation (dropped message!)");
            return true;
        }

        if (pool.forward(m)) {
            return true;
        }

        if (enqueueOnFail) {
            logger.error("ERROR: failed to forward message to remote constellation " + target + " (will retry!)");
            delivery.enqueue(m);
            return true;
        }

        logger.error("ERROR: failed to forward message to remote constellation " + target + " (may retry)");
        return false;
    }

    boolean handleStealReply(StealReply m) {

        // Handle a steal reply (bottom up)
        ConstellationIdentifier target = m.target;

        // Sanity check
        if (cidFactory.isLocal(target)) {
            logger.error("INTERNAL ERROR: received steal reply for local constellation (reclaiming work and dropped reply)");
            return false;
        }

        if (!pool.forward(m)) {
            // If the send fails we reclaim the work.

            if (!m.isEmpty()) {
                System.out.println("FAILED to deliver steal reply to " + target + " (reclaiming work and dropping reply)");
                logger.warn("FAILED to deliver steal reply to " + target + " (reclaiming work and dropping reply)");
                return false;
            } else {
                System.out.println("FAILED to deliver empty steal reply to " + target + " (dropping reply)");
                logger.warn("FAILED to deliver empty steal reply to " + target + " (dropping reply)");
            }
        }

        return true;
    }

    ConstellationIdentifierFactory getConstellationIdentifierFactory(
            ConstellationIdentifier cid) {
        return cidFactory;
    }

    synchronized void register(MultiThreadedConstellation c) throws Exception {

        if (active || subConstellation != null) {
            throw new Exception("Cannot register BottomConstellation");
        }

        subConstellation = c;
    }

    void belongsTo(StealPool belongsTo) {

        if (belongsTo == null) {
            logger.error("Constellation does not belong to any pool!");
            return;
        }

        if (belongsTo.isNone()) {
            // We don't belong to any pool. As a result, no one can steal from us.
            return;
        }

        if (belongsTo.isSet()) {

            StealPool [] set = belongsTo.set();

            for (int i=0;i<set.length;i++) {

                if (!set[i].isWorld()) {
                    pool.registerWithPool(set[i].getTag());
                }
            }

        } else {
            pool.registerWithPool(belongsTo.getTag());
        }
    }

   void stealsFrom(StealPool stealsFrom) {

        if (stealsFrom == null) {
            logger.warn("Constellation does not steal from to any pool!");
            return;
        }

        if (stealsFrom.isNone()) {
            // We don't belong to any pool. As a result, no one can steal from us.
            return;
        }

        if (stealsFrom.isSet()) {

            StealPool [] set = stealsFrom.set();

            for (int i=0;i<set.length;i++) {
                pool.followPool(set[i].getTag());
            }

        } else {
            pool.followPool(stealsFrom.getTag());
        }
    }
}
