package ibis.constellation.impl.dist;

import ibis.constellation.ConstellationIdentifier;
import ibis.constellation.StealPool;
import ibis.constellation.extra.ConstellationLogger;
import ibis.constellation.extra.Debug;
import ibis.constellation.extra.StealPoolInfo;
import ibis.constellation.impl.ApplicationMessage;
import ibis.constellation.impl.CombinedMessage;
import ibis.constellation.impl.LookupReply;
import ibis.constellation.impl.LookupRequest;
import ibis.constellation.impl.Message;
import ibis.constellation.impl.StealReply;
import ibis.constellation.impl.StealRequest;
import ibis.constellation.impl.UndeliverableEvent;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Random;
import java.util.Properties;

public class Pool implements RegistryEventHandler, MessageUpcall {

    private static final byte OPCODE_MESSAGE               = 42;
    
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

            // Dequeue in LIFO order too prevent unnec. updates
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

        String tmp = p.getProperty("ibis.cohort.master", "auto");

        if (tmp.equalsIgnoreCase("auto") || tmp.equalsIgnoreCase("true")) {
            // Elect a server
            master = ibis.registry().elect("Cohort Master");
        } else if (tmp.equalsIgnoreCase("false")) { 
            master = ibis.registry().getElectionResult("Cohort Master");
        } 

        if (master == null) {
            throw new Exception("Failed to find master!");
        }

        // We determine our rank here. This rank should only be used for 
        // debugging purposes!
        tmp = System.getProperty("ibis.cohort.rank");

        if (tmp != null) {
            try {
                rank = Long.parseLong(tmp);
            } catch (Exception e) {
                System.err.println("Failed to parse rank: " + tmp);
                rank = -1;            
            }
        } 

        if (rank == -1) { 
            rank = ibis.registry().getSequenceNumber("cohort-pool-" 
                    + master.toString());
        }

        isMaster = local.equals(master);

        rp = ibis.createReceivePort(portType, "cohort", this);
        rp.enableConnections();
        rp.enableMessageUpcalls();

        cidFactory = new DistributedConstellationIdentifierFactory(rank);

        locationCache.put((int)rank, local);
        
        // Register my rank at the master
        if (!isMaster) { 
            forwardInt(master, OPCODE_RANK_REGISTER_REQUEST, (int)rank);
        }
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

            } else if (m instanceof ApplicationMessage) { 

                logger.warn("POOL processing PENDING ApplicationMessage from " + m.source);
                owner.deliverRemoteEvent((ApplicationMessage)m);

            } else if (m instanceof UndeliverableEvent) { 

                logger.warn("POOL processing PENDING UndeliverableEvent from " + m.source);
                owner.deliverUndeliverableEvent((UndeliverableEvent)m);

            } else { 
                // Should never happen!
                logger.warning("POOL DROP unknown pending message ! " + m);
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

        synchronized (this) {
            if (!active) { 
                logger.warn("POOL Attempted getSendPort while inactive!", new Exception());
                return null;
            }
        }

        synchronized (sendports) {
            SendPort sp = sendports.get(id);

            if (sp == null) { 

                logger.warn("Connecting to " + id + " from " + ibis.identifier());

                System.err.println("Connecting to " + id + " from " + ibis.identifier());

                try {
                    sp = ibis.createSendPort(portType);
                    sp.connect(id, "cohort");
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
    
    private IbisIdentifier translate(ConstellationIdentifier id) { 
        int rank = (int)((id.id >> 32) & 0xffffffff);        
        return lookupRank(rank);
    }

    private boolean forward(IbisIdentifier id, byte opcode, Object data) { 

        SendPort s = getSendPort(id); 

        if (s == null) { 
            logger.warning("POOL failed to connect to " + id); 
            return false;
        }

        try { 
            WriteMessage wm = s.newMessage();
            wm.writeByte(opcode);
            wm.writeObject(data);
            wm.finish();
        } catch (Exception e) {
            logger.warning("POOL lost communication to " + id, e); 
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
            logger.warning("POOL failed to connect to " + id); 
            return false;
        }

        try { 
            WriteMessage wm = s.newMessage();
            wm.writeByte(opcode);
            wm.writeInt(data1);
            wm.writeObject(data2);
            wm.finish();
        } catch (Exception e) {
            logger.warning("POOL lost communication to " + id, e); 
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
            logger.warning("POOL failed to connect to " + id); 
            return false;
        }

        try { 
            WriteMessage wm = s.newMessage();
            wm.writeByte(opcode);
            wm.writeInt(data);
            wm.finish();
        } catch (Exception e) {
            logger.warning("POOL lost communication to " + id, e); 
            return false;
        }

        synchronized (this) {
            send++;
        }

        return true;
    }
    
    private boolean forward(IbisIdentifier id, Message m) { 
        return forward(id, OPCODE_MESSAGE, m);
    }


    public boolean forward(Message m) { 

        ConstellationIdentifier target = m.target;

        if (Debug.DEBUG_COMMUNICATION) { 
            logger.info("POOL FORWARD Message from " + m.source + " to " + m.target + " " + m);
        }

        IbisIdentifier id = translate(target);

        if (id == null) { 
            logger.warning("POOL failed to translate " + target 
                    + " to an IbisIdentifier");
            return false;
        }

        return forward(id, OPCODE_MESSAGE, m);
    }

    public boolean forwardToMaster(Message m) { 
        return forward(master, m);
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

            ((StealRequest)m).setAllowRestricted();
            owner.deliverRemoteStealRequest((StealRequest)m);
            return true;

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

        } else if (m instanceof StealReply) {

            if (Debug.DEBUG_COMMUNICATION || Debug.DEBUG_STEAL) { 
                logger.info("POOL RECEIVE StealReply from " + m.source);
            }

            owner.deliverRemoteStealReply((StealReply)m);

        } else if (m instanceof LookupReply) { 

            if (Debug.DEBUG_COMMUNICATION || Debug.DEBUG_LOOKUP) { 
                logger.info("POOL RECEIVE LookupReply from " + m.source);
            }

            owner.deliverRemoteLookupReply((LookupReply)m);

        } else if (m instanceof ApplicationMessage) { 

            if (Debug.DEBUG_COMMUNICATION || Debug.DEBUG_EVENTS) { 
                logger.info("POOL RECEIVE EventMessage from " + m.source);
            }

            owner.deliverRemoteEvent((ApplicationMessage)m);

        } else if (m instanceof UndeliverableEvent) { 

            if (Debug.DEBUG_COMMUNICATION || Debug.DEBUG_EVENTS) { 
                logger.info("POOL RECEIVE UndeliverableEvent from " + m.source);
            }

            owner.deliverUndeliverableEvent((UndeliverableEvent)m);

        } else { 
            // Should never happen!
            logger.warning("POOL DROP unknown message type! " + m);
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
        
        forward(src, OPCODE_RANK_LOOKUP_REPLY, rank, src);
    }
    
    public void upcall(ReadMessage rm) throws IOException, ClassNotFoundException {

        byte opcode = rm.readByte();

        switch (opcode) {
        case OPCODE_MESSAGE: {
            Message m = (Message) rm.readObject();

            // FIXME: EXPENSIVE!
            synchronized (this) {
                received++;

                if (!active) { 
                    logger.warn("POOL Received message while inactive!", new Exception());
                    addPendingMessage(m);
                    return;
                }
            }

            handleMessage(m, rm);
        }
        break;

        case OPCODE_POOL_REGISTER_REQUEST: {            
            String tag = (String) rm.readObject();
            IbisIdentifier source = rm.origin().ibisIdentifier();
            performRegisterWithPool(source, tag);
        }
        break;

        case OPCODE_POOL_UPDATE_REQUEST: {
            String tag = (String) rm.readObject();
            IbisIdentifier source = rm.origin().ibisIdentifier();
            performRegisterWithPool(source, tag);
            rm.finish();            
            performUpdateRequest(source, tag);            
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

    public boolean randomForwardToPool(StealRequest sr) {

        //	System.out.println("RANDOM FORWARD TO POOL " + sr.pool.getTag());

        // NOTE: We know the pool is not NULL, WORLD or NONE. 
        StealPool pool = sr.pool;

        if (pool.isSet()) {             
            StealPool [] tmp = pool.set();            
            pool = tmp[random.nextInt(tmp.length)];                        
        }

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

        return forward(id, sr);
    } 


    private void performRegisterWithPool(IbisIdentifier id, String tag) {

        PoolInfo tmp = null;

        System.err.println("Processing register request " + tag + " from " + id);

        synchronized (pools) {
            tmp = pools.get(tag);
        }

        if (tmp == null) { 
            logger.warn("Failed to find pool " + tag + " to register " + id); 
            System.err.println("Failed to find pool " + tag + " to register " + id); 
            return;
        }

        tmp.addMember(id);        
    }

    private void performUpdateRequest(IbisIdentifier id, String tag) {

        PoolInfo tmp = null;

        synchronized (pools) {
            tmp = pools.get(tag);
        }

        if (tmp == null) { 
            logger.warn("Failed to find pool " + tag + " to register " + id); 
            return;
        }

        forward(id, OPCODE_POOL_UPDATE_REPLY, tmp);
    }


    private void requestRegisterWithPool(IbisIdentifier master, String tag) {

        System.err.println("Sending register request for pool " + tag + " to " + master);

        forward(master, OPCODE_POOL_REGISTER_REQUEST, tag);
    }

    private void requestUpdate(IbisIdentifier master, String tag) {

        System.err.println("Sending update request for pool " + tag + " to " + master);

        forward(master, OPCODE_POOL_UPDATE_REQUEST, tag);
    }

    public void registerWithPool(String tag) { 

        // NOTE: We know that tag is not NULL, WORLD or NONE.

        // TODO: we currently assume that this function is called once for each tag ?

        try {
            // Simple case: we are part of the pool or already known it.
            Registry reg = ibis.registry();

            String electTag = "STEALPOOL$" + tag;

            System.err.println("Electing master for POOL " + electTag);

            IbisIdentifier id = reg.elect(electTag);

            // NOTE: there may be a race here between the election and the hashmap!! 
            boolean master = id.equals(ibis.identifier()); 

            System.err.println("Master for POOL " + electTag + " is " + id + " " + master);

            if (master) { 

                synchronized (pools) {

                    PoolInfo info = pools.get(tag);

                    if (info != null) { 
                        System.err.println("Hit race in pool registration!");
                    } else { 
                        info = new PoolInfo(tag, id, master);
                        pools.put(tag, info);
                    }
                }
            } else { 
                requestRegisterWithPool(id, tag);
            }

        } catch (IOException e) {
            logger.warn("Failed to register pool " + tag, e);

            System.err.println("Failed to register pool " + tag + " " + e);
            e.printStackTrace(System.err);
        }
    }

    public void followPool(String tag) { 

        // NOTE: race condition between elect and hashmap.add!
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

            System.err.println("Searching master for POOL " + electTag);

            // NOTE: this may hang or return null ??
            IbisIdentifier id = reg.getElectionResult(electTag);

            boolean master = id.equals(ibis.identifier());

            System.err.println("Found master for POOL " + electTag + " " + id + " " + master);

            if (master) { 
                // Assuming the pools are static and registered in 
                // the right order this should not happen
                logger.error("INTERNAL ERROR: election of follow pool returned self!");
                return;
            }

            PoolInfo info = new PoolInfo(tag, id, false);

            synchronized (pools) {
                pools.put(tag, info);
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

            if (info.timestamp > tmp.timestamp) { 
                pools.put(info.tag, info);
            }
        }
    }

    private void requestUpdate(String tag) { 

        IbisIdentifier master = null;

        System.err.println("Requesting update for pool " + tag);

        synchronized (pools) {
            PoolInfo tmp = pools.get(tag);

            if (tmp == null) { 
                logger.warn("Cannot request update for " + tag + ": unknown pool!");
                System.err.println("Cannot request update for " + tag + ": unknown pool!");
                return;
            }

            master = tmp.master;
        }

        requestUpdate(master, tag);
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
