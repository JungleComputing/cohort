package ibis.constellation.impl;

import ibis.constellation.ConstellationIdentifier;
import ibis.constellation.StealPool;
import ibis.constellation.extra.ConstellationLogger;
import ibis.constellation.extra.Debug;
import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.MessageUpcall;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.Registry;
import ibis.ipl.RegistryEventHandler;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Random;

public class Pool implements RegistryEventHandler, MessageUpcall {

    private static final byte OPCODE_EVENT_MESSAGE         = 10;
    private static final byte OPCODE_STEAL_REQUEST         = 11;
    private static final byte OPCODE_STEAL_REPLY           = 12;

    private static final byte OPCODE_POOL_REGISTER_REQUEST = 43;
    private static final byte OPCODE_POOL_UPDATE_REQUEST   = 44;
    private static final byte OPCODE_POOL_UPDATE_REPLY     = 45;

    private static final byte OPCODE_RANK_REGISTER_REQUEST = 53;
    private static final byte OPCODE_RANK_LOOKUP_REQUEST   = 54;
    private static final byte OPCODE_RANK_LOOKUP_REPLY     = 55;

    private DistributedConstellation owner;

    private final PortType portType = new PortType(
            PortType.COMMUNICATION_FIFO,
            PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_OBJECT,
            PortType.RECEIVE_AUTO_UPCALLS,
            PortType.RECEIVE_TIMEOUT,
            PortType.CONNECTION_MANY_TO_ONE);

    private static final IbisCapabilities ibisCapabilities =
        new IbisCapabilities(
                IbisCapabilities.MALLEABLE,
                IbisCapabilities.TERMINATION,
                IbisCapabilities.ELECTIONS_STRICT,
                IbisCapabilities.MEMBERSHIP_TOTALLY_ORDERED);

    private final ReceivePort rp;

    private final HashMap<IbisIdentifier, SendPort> sendports =
        new HashMap<IbisIdentifier, SendPort>();

    private final HashMap<Integer, IbisIdentifier> locationCache =
        new HashMap<Integer, IbisIdentifier>();

    private final DistributedConstellationIdentifierFactory cidFactory;

    private ConstellationLogger logger;

    private final Ibis ibis;
    private final IbisIdentifier local;
    private IbisIdentifier master;

    private long rank = -1;

    private boolean isMaster;

    private final Random random = new Random();

    private long received;
    private long send;

    private boolean active = false;

    //private StealPoolInfo poolInfo = new StealPoolInfo();

    private LinkedList<Message> pending = new LinkedList<Message>();

    class PoolUpdater extends Thread {

        private static final long MIN_DELAY = 1000;
        private static final long MAX_DELAY = 10000;

        private long deadline = 0;
        private long currentDelay = MIN_DELAY;
        private boolean done;

        private ArrayList<String> tags = new ArrayList<String>();
        private ArrayList<PoolInfo> updates = new ArrayList<PoolInfo>();

        public synchronized void addTag(String tag) {
            if (!tags.contains(tag)) {
                tags.add(tag);
            }
        }

        public synchronized String [] getTags() {
            return tags.toArray(new String[tags.size()]);
        }

        public synchronized void enqueueUpdate(PoolInfo info) {
            updates.add(info);
            notifyAll();
        }

        public synchronized PoolInfo dequeueUpdate() {

            if (updates.size() == 0) {
                return null;
            }

            // Dequeue in LIFO order too prevent unnecessary updates
            return updates.remove(updates.size()-1);
        }

        private synchronized boolean getDone() {
            return done;
        }

        public synchronized void done() {
            done = true;
        }

        private void processUpdates() {

            PoolInfo update = dequeueUpdate();

            if (update == null) {
                // No updates
                currentDelay += MIN_DELAY;
               
                if (currentDelay >= MAX_DELAY) { 
                	currentDelay = MAX_DELAY;
                }
                
                return;
            }

            currentDelay = MIN_DELAY;

            while (update != null) {
                performUpdate(update);
                update = dequeueUpdate();
            }
        }

        private void sendUpdateRequests() {

            String [] pools = getTags();

            for (int i=0;i<pools.length;i++) {
                requestUpdate(pools[i]);
            }
        }

        private void waitUntilDeadLine() {

            long sleep = deadline-System.currentTimeMillis();

            if (sleep > 0) {
                try {
                    synchronized (this) {
                        wait(sleep);
                    }
                } catch (Exception e) {
                    // ignore
                }
            }
        }

        public void run() {

        	while (!getDone()) {

                processUpdates();

                long now = System.currentTimeMillis();

                if (now >= deadline) {
                    sendUpdateRequests();
                    deadline = now + currentDelay;
                }

                waitUntilDeadLine();
            }
        }
    }

    private HashMap<String, PoolInfo> pools = new HashMap<String, PoolInfo>();
    private PoolUpdater updater = new PoolUpdater();

    public Pool(final DistributedConstellation owner, final Properties p) throws Exception {

        this.owner = owner;
        this.logger = ConstellationLogger.getLogger(Pool.class, null);

        ibis = IbisFactory.createIbis(ibisCapabilities, p, true, this, portType);

        local = ibis.identifier();

        ibis.registry().enableEvents();

        String tmp = p.getProperty("ibis.constellation.master", "auto");

        if (tmp.equalsIgnoreCase("auto") || tmp.equalsIgnoreCase("true")) {
            // Elect a server
            master = ibis.registry().elect("Constellation Master");
        } else if (tmp.equalsIgnoreCase("false")) {
            master = ibis.registry().getElectionResult("Constellation Master");
        }

        if (master == null) {
            throw new Exception("Failed to find master!");
        }

        // We determine our rank here. This rank should only be used for
        // debugging purposes!
        tmp = System.getProperty("ibis.constellation.rank");

        if (tmp != null) {
            try {
                rank = Long.parseLong(tmp);
            } catch (Exception e) {
                System.err.println("Failed to parse rank: " + tmp);
                rank = -1;
            }
        }

        if (rank == -1) {
            rank = ibis.registry().getSequenceNumber("constellation-pool-"
                    + master.toString());
        }

        isMaster = local.equals(master);

        rp = ibis.createReceivePort(portType, "constellation", this);
        rp.enableConnections();
        rp.enableMessageUpcalls();

        cidFactory = new DistributedConstellationIdentifierFactory(rank);

        locationCache.put((int)rank, local);

        // Register my rank at the master
        if (!isMaster) {
            forwardInt(master, OPCODE_RANK_REGISTER_REQUEST, (int)rank);
        }
        
        // Start the updater thread...
        updater.start();
    }

    protected void setLogger(ConstellationLogger logger) {
        this.logger = logger;
        logger.info("Cohort master is " + master + " rank is " + rank);
    }

    public void activate() {
        logger.warn("Activating POOL on " + ibis.identifier());

        synchronized (this) {
            active = true;
        }

        processPendingMessages();
    }

    private void processPendingMessages() {

        while (pending.size() > 0) {
            Message m = pending.removeFirst();

            if (m instanceof StealRequest) {

                logger.warn("POOL processing PENDING StealRequest from " + m.source);
                owner.deliverRemoteStealRequest((StealRequest)m);

            } else if (m instanceof EventMessage) {

                logger.warn("POOL processing PENDING ApplicationMessage from " + m.source);
                owner.deliverRemoteEvent((EventMessage)m);
/*
            } else if (m instanceof UndeliverableEvent) {

                logger.warn("POOL processing PENDING UndeliverableEvent from " + m.source);
                owner.deliverUndeliverableEvent((UndeliverableEvent)m);
*/
            } else {
                // Should never happen!
                logger.warn("POOL DROP unknown pending message ! " + m);
            }
        }
    }

    /*
    public synchronized CohortIdentifier generateCohortIdentifier(int worker) {
        return new DistributedCohortIdentifier(local, rank, worker);
    }
     *///

    public DistributedConstellationIdentifierFactory getCIDFactory() {
        return cidFactory;
    }

    public boolean isLocal(ConstellationIdentifier id) {
        return (rank << 32 ^ id.id) == 0;
    }

    private SendPort getSendPort(IbisIdentifier id) {

        // TODO: not fault tolerant!!!
        if (id.equals(ibis.identifier())) {
            logger.fatal("POOL Sending to myself!", new Exception());
        }

        synchronized (sendports) {
            SendPort sp = sendports.get(id);

            if (sp == null) {

                logger.warn("Connecting to " + id + " from " + ibis.identifier());

                System.out.println("Connecting to " + id + " from " + ibis.identifier());

                try {
                    sp = ibis.createSendPort(portType);
                    sp.connect(id, "constellation");
                } catch (IOException e) {

                    try {
                        sp.close();
                    } catch (Exception e2) {
                        // ignored ?
                    }

                    e.printStackTrace();
                    return null;
                }

                logger.warn("Succesfully connected to " + id + " from " + ibis.identifier());

                sendports.put(id, sp);
            }

            return sp;
        }
    }
    /*
    private void releaseSendPort(IbisIdentifier id, SendPort sp) {
        // empty
    }
     */

    public void died(IbisIdentifier id) {
        left(id);
    }

    public void electionResult(String name, IbisIdentifier winner) {
        // ignored ?
    }

    public void gotSignal(String signal, IbisIdentifier source) {
        // ignored
    }

    public void joined(IbisIdentifier id) {

        //synchronized (others) {
        //    if (!id.equals(local)) {
        //        others.add(id);
        //        logger.warn("JOINED " + id);
        //    }
        // }
    }

    public void left(IbisIdentifier id) {

        //synchronized (locationCache) {
        //    others.remove(id);
        //}

        synchronized (sendports) {
            sendports.remove(id);
        }
    }

    public void poolClosed() {
        // ignored
    }

    public void poolTerminated(IbisIdentifier id) {
        // ignored
    }

    public void terminate() throws IOException {
        if (isMaster) {
            ibis.registry().terminate();
        } else {
            ibis.registry().waitUntilTerminated();
        }
    }

    public void cleanup() {

        try {
            ibis.end();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public long getRank() {
        return rank;
    }

    public boolean isMaster() {
        return isMaster;
    }

    /*
    private IbisIdentifier selectRandomTarget() {

        synchronized (others) {

            int size = others.size();

            if (size == 0) {
                return null;
            }

            return others.get(random.nextInt(size));
        }
    }
     */

    private IbisIdentifier translate(ConstellationIdentifier cid) {
        int rank = (int)((cid.id >> 32) & 0xffffffff);
        return lookupRank(rank);
    }

    private boolean doForward(IbisIdentifier id, byte opcode, Object data) {

        SendPort s = getSendPort(id);

        if (s == null) {
            logger.warn("POOL failed to connect to " + id);
            return false;
        }

        try {
            WriteMessage wm = s.newMessage();
            wm.writeByte(opcode);
            wm.writeObject(data);
            wm.finish();
        } catch (Exception e) {
            logger.warn("POOL lost communication to " + id, e);
            return false;
        }

        synchronized (this) {
            send++;
        }

        return true;
    }

    private boolean forward(IbisIdentifier id, byte opcode, int data1, Object data2) {

        SendPort s = getSendPort(id);

        if (s == null) {
            logger.warn("POOL failed to connect to " + id);
            return false;
        }

        try {
            WriteMessage wm = s.newMessage();
            wm.writeByte(opcode);
            wm.writeInt(data1);
            wm.writeObject(data2);
            wm.finish();
        } catch (Exception e) {
            logger.warn("POOL lost communication to " + id, e);
            return false;
        }

        synchronized (this) {
            send++;
        }

        return true;
    }

    private boolean forwardInt(IbisIdentifier id, byte opcode, int data) {

        SendPort s = getSendPort(id);

        if (s == null) {
            logger.warn("POOL failed to connect to " + id);
            return false;
        }

        try {
            WriteMessage wm = s.newMessage();
            wm.writeByte(opcode);
            wm.writeInt(data);
            wm.finish();
        } catch (Exception e) {
            logger.warn("POOL lost communication to " + id, e);
            return false;
        }

        synchronized (this) {
            send++;
        }

        return true;
    }

    public boolean forward(StealReply sr) {

     //   System.out.println("POOL:FORWARD StealReply from " + sr.source + " to " + sr.target);

        return forward(sr, OPCODE_STEAL_REPLY);
    }

    public boolean forward(EventMessage em) {

    //    System.out.println("POOL:FORWARD EventMessage from " + em.source + " to " + em.target + " target " + em.event.target);

        return forward(em, OPCODE_EVENT_MESSAGE);
    }

    private boolean forward(Message m, byte opcode) {

        ConstellationIdentifier target = m.target;

        if (Debug.DEBUG_COMMUNICATION) {
            logger.info("POOL FORWARD Message from " + m.source + " to " + m.target + " " + m);
        }

        IbisIdentifier id = translate(target);

        if (id == null) {
            System.out.println("POOL failed to translate " + target + " to an IbisIdentifier");

            logger.warn("POOL failed to translate " + target
                    + " to an IbisIdentifier");
            return false;
        }

        return doForward(id, opcode, m);
    }

    public boolean forwardToMaster(StealRequest m) {
        return doForward(master, OPCODE_STEAL_REQUEST, m);
    }

    /*
    public boolean randomForward(Message m) {

        IbisIdentifier rnd = selectRandomTarget();

        if (rnd == null) {
            logger.warning("POOL failed to randomly select target: " +
            "no other cohorts found");
            return false;
        }

        forward(rnd, OPCODE_MESSAGE, m);

        return true;
    }
     */

    public ConstellationIdentifier selectTarget() {
        return null;
    }

    private void addPendingMessage(Message m) {

        /*
        if (m instanceof StealReply) {
            StealReply sr = (StealReply) m;

            if (!sr.isEmpty()) {
                logger.warn("POOL(INACTIVE) SAVING StealReply with WORK from " + m.source);
                pending.add(m);
            } else {
                logger.warn("POOL(INACTIVE) DROPPING Empty stealReply from " + m.source);
            }
        } else if (m instanceof ApplicationMessage) {
            logger.warn("POOL(INACTIVE) SAVING ApplicationMessage from " + m.source);
            pending.add(m);
        } else if (m instanceof UndeliverableEvent) {
            logger.warn("POOL(INACTIVE) SAVING UndeliverableEvent from " + m.source);
            pending.add(m);
        } else {
            logger.warn("POOL(INACTIVE) DROPPING Message from " + m.source );
        }*/

        pending.add(m);
    }

    private boolean handleMessage(Message m, ReadMessage rm) throws IOException {

        if (m instanceof StealRequest) {
            // This method may result in a nested call to send. Therefore we
            // need to finish the ReadMessage first to allow 'stealing' of the
            // upcall thread.

            if (Debug.DEBUG_COMMUNICATION || Debug.DEBUG_STEAL) {
                logger.info("POOL RECEIVE StealRequest from " + m.source);
            }

            if (rm != null) {
                rm.finish();
            }

            ((StealRequest)m).setRemote();
            owner.deliverRemoteStealRequest((StealRequest)m);
            return true;

/*
        } else if (m instanceof LookupRequest) {
            // This method may result in a nested call to send. Therefore we
            // need to finish the ReadMessage first to allow 'stealing' of the
            // upcall thread.

            if (Debug.DEBUG_COMMUNICATION || Debug.DEBUG_LOOKUP) {
                logger.info("POOL RECEIVE LookupRequest from " + m.source);
            }

            if (rm != null) {
                rm.finish();
            }
            owner.deliverRemoteLookupRequest((LookupRequest)m);
            return true;
*/
        } else if (m instanceof StealReply) {

            if (Debug.DEBUG_COMMUNICATION || Debug.DEBUG_STEAL) {
                logger.info("POOL RECEIVE StealReply from " + m.source);
            }

            owner.deliverRemoteStealReply((StealReply)m);

/*
        } else if (m instanceof LookupReply) {

            if (Debug.DEBUG_COMMUNICATION || Debug.DEBUG_LOOKUP) {
                logger.info("POOL RECEIVE LookupReply from " + m.source);
            }

            owner.deliverRemoteLookupReply((LookupReply)m);
*/
        } else if (m instanceof EventMessage) {

            if (Debug.DEBUG_COMMUNICATION || Debug.DEBUG_EVENTS) {
                logger.info("POOL RECEIVE EventMessage from " + m.source);
            }

            owner.deliverRemoteEvent((EventMessage)m);
/*
        } else if (m instanceof UndeliverableEvent) {

            if (Debug.DEBUG_COMMUNICATION || Debug.DEBUG_EVENTS) {
                logger.info("POOL RECEIVE UndeliverableEvent from " + m.source);
            }

            owner.deliverUndeliverableEvent((UndeliverableEvent)m);
*/
        } else {
            // Should never happen!
            logger.warn("POOL DROP unknown message type! " + m);
        }

        return false;
    }

    private void registerRank(int rank, IbisIdentifier src) {

        synchronized (locationCache) {
            IbisIdentifier old = locationCache.put(rank, src);

            if (old != null) {
                // sanity check

                if (!old.equals(src)) {
                    logger.error("ERROR: Location cache overwriting rank "
                            + rank + " with different id! "
                            + old + " != " + src);
                }
            }
        }
    }

    private IbisIdentifier lookupRankLocally(int rank) {

        synchronized (locationCache) {
            return locationCache.get(rank);
        }
    }

    public IbisIdentifier lookupRank(int rank) {

    //    System.out.println("Lookup of rank " + rank);

        IbisIdentifier tmp;

        // Do a local lookup
        synchronized (locationCache) {
            tmp = locationCache.get(rank);
        }

        // Return if we have a result, or if there is no one that we can ask
        if (tmp != null || isMaster) {
            return tmp;
        }

        // Forward a request to the master for the 'IbisID' of 'rank'
        forwardInt(master, OPCODE_RANK_LOOKUP_REQUEST, rank);

        return null;
    }

    private void lookupRankRequest(int rank, IbisIdentifier src) {

        IbisIdentifier tmp = lookupRankLocally(rank);

        if (tmp == null) {
            logger.warn("Location lookup for rank "
                    + rank + " returned null!");
        }

        forward(src, OPCODE_RANK_LOOKUP_REPLY, rank, tmp);
    }

    public void upcall(ReadMessage rm) throws IOException, ClassNotFoundException {

        byte opcode = rm.readByte();

        switch (opcode) {
        case OPCODE_STEAL_REQUEST:
        {
            StealRequest m = (StealRequest) rm.readObject();

            if (Debug.DEBUG_COMMUNICATION || Debug.DEBUG_STEAL) {
                logger.info("POOL RECEIVE StealRequest from " + m.source);
            }

            rm.finish();

            m.setRemote();
            owner.deliverRemoteStealRequest(m);
        }
        break;

        case OPCODE_STEAL_REPLY:
        {
            StealReply m = (StealReply) rm.readObject();

            // FIXME: EXPENSIVE!
            synchronized (this) {
                received++;

                if (!active) {
                    logger.warn("POOL Received message while inactive!", new Exception());
                    addPendingMessage(m);
                    return;
                }
            }

            if (Debug.DEBUG_COMMUNICATION || Debug.DEBUG_STEAL) {
                logger.info("POOL RECEIVE StealReply from " + m.source);
            }

            owner.deliverRemoteStealReply(m);
        }
        break;

        case OPCODE_EVENT_MESSAGE:
        {
            EventMessage m = (EventMessage) rm.readObject();

            // FIXME: EXPENSIVE!
            synchronized (this) {
                received++;

                if (!active) {
                    logger.warn("POOL Received message while inactive!", new Exception());
                    addPendingMessage(m);
                    return;
                }
            }

            if (Debug.DEBUG_COMMUNICATION || Debug.DEBUG_EVENTS) {
                logger.info("POOL RECEIVE EventMessage from " + m.source);
            }

            rm.finish();

            owner.deliverRemoteEvent((EventMessage)m);
        }
        break;

        case OPCODE_POOL_REGISTER_REQUEST: {
            String tag = (String) rm.readObject();
            IbisIdentifier source = rm.origin().ibisIdentifier();
            performRegisterWithPool(source, tag);
        }
        break;

        case OPCODE_POOL_UPDATE_REQUEST: {
            PoolUpdateRequest req = (PoolUpdateRequest) rm.readObject();
            IbisIdentifier source = rm.origin().ibisIdentifier();
            rm.finish();
            performUpdateRequest(source, req);
        }
        break;

        case OPCODE_POOL_UPDATE_REPLY: {
            PoolInfo tmp = (PoolInfo) rm.readObject();
            updater.enqueueUpdate(tmp);
        }
        break;

        case OPCODE_RANK_REGISTER_REQUEST: {
            int rank = rm.readInt();
            IbisIdentifier source = rm.origin().ibisIdentifier();
            registerRank(rank, source);
        }
        break;

        case OPCODE_RANK_LOOKUP_REQUEST: {
            int rank = rm.readInt();
            IbisIdentifier source = rm.origin().ibisIdentifier();
            rm.finish();
            lookupRankRequest(rank, source);
        }
        break;

        case OPCODE_RANK_LOOKUP_REPLY: {
            int rank = rm.readInt();
            IbisIdentifier id = (IbisIdentifier) rm.readObject();
            registerRank(rank, id);
        }
        break;

        default:
            logger.error("Received unknown message opcode: " + opcode);
        }

        /*
        if (m instanceof CombinedMessage) {

            Message [] messages = ((CombinedMessage)m).getMessages();

            for (int i=0;i<messages.length;i++) {
                boolean rmFinished = handleMessage(messages[i], rm);

                if (rmFinished) {
                    rm = null;
                }
            }
        } else {
            handleMessage(m, rm);
        }
         */
    }

    /*
    public void broadcast(Message m) {

        // This seems to produce many problems...
        int size = 0;

        synchronized (others) {
            size = others.size();
        }

        for (int i=0;i<size;i++) {

            IbisIdentifier tmp = null;

            synchronized (others) {
                if (i < others.size()) {
                    tmp = others.get(i);
                }
            }

            if (tmp == null) {
                logger.warning("POOL failed to retrieve Ibis " + i);
            } else {
                forward(tmp, OPCODE_MESSAGE, m);
            }
        }
    }*/

    public boolean randomForwardToPool(StealPool pool, StealRequest sr) {

        // NOTE: We know the pool sp is not NULL or NONE, and not a set
        PoolInfo info = null;

        synchronized (pools) {
            info = pools.get(pool.getTag());
        }

        IbisIdentifier id = null;

        if (info != null) {
            id = info.selectRandom(random);
        }

        if (id == null) {
            logger.warn("Failed to randomly select node in pool " + pool.getTag());
            return false;
        }

        if (id.equals(local)) {
            return false;
        }

        return doForward(id, OPCODE_STEAL_REQUEST, sr);
    }

    public StealPool randomlySelectPool(StealPool pool) {

        // NOTE: We know the pool is not NULL or NONE.
        if (pool.isSet()) {
            StealPool [] tmp = pool.set();
            pool = tmp[random.nextInt(tmp.length)];
        }

        return pool;
    }

    private void performRegisterWithPool(IbisIdentifier id, String tag) {

        PoolInfo tmp = null;

        System.err.println("Processing register request " + tag + " from " + id);

        synchronized (pools) {
            tmp = pools.get(tag);
        }

        if (tmp == null) {
            logger.error("Failed to find pool " + tag + " to register " + id);
            System.err.println("Failed to find pool " + tag + " to register " + id);
            return;
        }

        tmp.addMember(id);
    }

    private void performUpdateRequest(IbisIdentifier id, PoolUpdateRequest request) {

        PoolInfo tmp = null;

        synchronized (pools) {
            tmp = pools.get(request.tag);
        }

        if (tmp == null) {
            logger.warn("Failed to find pool " + request.tag + " for update request from " + id);
            return;
        }

        if (tmp.currentTimeStamp() > request.timestamp) { 
        	doForward(id, OPCODE_POOL_UPDATE_REPLY, tmp);
        } else { 
        	logger.info("No updates found for pool " + request.tag + " / " + request.timestamp);
        }
    }

    private void requestRegisterWithPool(IbisIdentifier master, String tag) {

        System.err.println("Sending register request for pool " + tag + " to " + master);

        doForward(master, OPCODE_POOL_REGISTER_REQUEST, tag);
    }

    private void requestUpdate(IbisIdentifier master, String tag, long timestamp) {

        System.err.println("Sending update request for pool " + tag + " to " + master + " for timestamp " + timestamp);

        doForward(master, OPCODE_POOL_UPDATE_REQUEST, new PoolUpdateRequest(tag, timestamp));
    }

    public void registerWithPool(String tag) {

    	try {
    		// First check if the pool is already registered. If not, we need to add a 
    		// temporary PoolInfo object to our hashmap to ensure that we catch any 
    		// register requests if we become the master!
    	    
    		synchronized (pools) {
    	    	PoolInfo info = pools.get(tag);

    	    	if (info != null) { 
    	            logger.info("Pool " + tag + " already registered!");
    	            return;
    	    	}
    	    	
    	    	pools.put(tag, new PoolInfo(tag));
    	    }
    		  
    	    // Next, elect a master for this pool.
    		Registry reg = ibis.registry();

            String electTag = "STEALPOOL$" + tag;

            logger.info("Electing master for POOL " + electTag);

            IbisIdentifier id = reg.elect(electTag);

            boolean master = id.equals(ibis.identifier());

            logger.info("Master for POOL " + electTag + " is " + id + " " + master);

            // Next, create the pool locally, or register ourselves at the master. 
            if (master) {

                synchronized (pools) {
                    PoolInfo info = pools.get(tag);

                    if (info.hasMembers()) {
                        logger.warn("Hit race in pool registration! -- will recover!");
                        pools.put(tag, new PoolInfo(info, id));
                    } else {
                        pools.put(tag, new PoolInfo(tag, id, true));
                    }
                }
            } else {
            	// We remove the unused PoolInfo
            	synchronized (pools) {
            		PoolInfo info = pools.remove(tag);

            		// Sanity checks
            		if (info != null) { 
            			if (!info.isDummy) { 
            				logger.error("INTERNAL ERROR: Removed non-dummy PoolInfo!");
            			}
            		} else { 
            			logger.warn("Failed to find dummy PoolInfo!");
                 	}
            	}
            	
                requestRegisterWithPool(id, tag);
            }

        } catch (IOException e) {
            logger.warn("Failed to register pool " + tag, e);

            System.err.println("Failed to register pool " + tag + " " + e);
            e.printStackTrace(System.err);
        }
    }

    public void followPool(String tag) {

        try {
            // Simple case: we own the pool or already follow it.
            synchronized (pools) {
                if (pools.containsKey(tag)) {
                    return;
                }
            }

            // Complex case: we are not part of the pool, but interested anyway
            Registry reg = ibis.registry();

            String electTag = "STEALPOOL$" + tag;

            IbisIdentifier id = null;
           
            // TODO: will repeat for ever if pool master does not exist...
            while (id == null) { 
            	System.err.println("Searching master for POOL " + electTag);
            	logger.info("Searching master for POOL " + electTag);
            	id = reg.getElectionResult(electTag, 1000);
            } 
            	
            boolean master = id.equals(ibis.identifier());

            logger.info("Found master for POOL " + electTag + " " + id + " " + master);

            if (master) {
                // Assuming the pools are static and registered in
                // the right order this should not happen
                logger.error("INTERNAL ERROR: election of follow pool returned self!");
                return;
            }

            synchronized (pools) {
                pools.put(tag, new PoolInfo(tag, id, false));
            }

            updater.addTag(tag);
        } catch (IOException e) {
            logger.warn("Failed to register pool " + tag, e);

            System.err.println("Failed to register pool " + tag + " " + e);
            e.printStackTrace(System.err);
        }
    }


    private void performUpdate(PoolInfo info) {

        synchronized (pools) {
            PoolInfo tmp = pools.get(info.tag);

            if (tmp == null) {
                logger.warn("Received spurious pool update! " + info.tag);
                System.err.println("Received spurious pool update! " + info.tag);
                return;
            }

            if (info.currentTimeStamp() > tmp.currentTimeStamp()) {
                pools.put(info.tag, info);
            }
        }
    }

    private void requestUpdate(String tag) {

    	PoolInfo tmp = null;
    	
    	logger.info("Requesting update for pool " + tag);

        synchronized (pools) {
            tmp = pools.get(tag);

            if (tmp == null || tmp.isDummy) {
                logger.warn("Cannot request update for " + tag + ": unknown pool!");
                System.err.println("Cannot request update for " + tag + ": unknown pool!");
                return;
            }

            master = tmp.master;
        }

        requestUpdate(tmp.master, tag, tmp.currentTimeStamp());
    }


    /*
        byte opcode = rm.readByte();

        switch (opcode) {
        case EVENT:
            Event e = (Event) rm.readObject();
            parent.deliverEvent(e);

            synchronized (this) {
                messagesReceived++;
                eventsReceived++;
            }
            break;

        case STEAL:
            StealRequest r = (StealRequest) rm.readObject();
            parent.incomingRemoteStealRequest(r);
            synchronized (this) {
                messagesReceived++;
                stealsReceived++;
            }
            break;

        case STEALREPLY:
            StealReply reply = (StealReply) rm.readObject();
            parent.incomingStealReply(reply);

            synchronized (this) {
                messagesReceived++;

                if (reply.work == null) {
                    no_workReceived++;
                } else {
                    workReceived++;
                }
            }
            break;

        default:
            throw new IOException("Unknown opcode: " + opcode);
        }*/

}
