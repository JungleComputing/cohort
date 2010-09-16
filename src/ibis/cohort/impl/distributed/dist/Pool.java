package ibis.cohort.impl.distributed.dist;

import ibis.cohort.CohortIdentifier;
import ibis.cohort.StealPool;
import ibis.cohort.extra.CohortLogger;
import ibis.cohort.extra.Debug;
import ibis.cohort.extra.StealPoolInfo;
import ibis.cohort.impl.distributed.ApplicationMessage;
import ibis.cohort.impl.distributed.CombinedMessage;
import ibis.cohort.impl.distributed.LookupReply;
import ibis.cohort.impl.distributed.LookupRequest;
import ibis.cohort.impl.distributed.Message;
import ibis.cohort.impl.distributed.StealReply;
import ibis.cohort.impl.distributed.StealRequest;
import ibis.cohort.impl.distributed.UndeliverableEvent;
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
    
    private DistributedCohort owner;

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

    private final ArrayList<IbisIdentifier> others = 
        new ArrayList<IbisIdentifier>();

    private final DistributedCohortIdentifierFactory cidFactory;

    private CohortLogger logger;

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
            tags.add(tag);            
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
    
    public Pool(final DistributedCohort owner, final Properties p) throws Exception { 

        this.owner = owner; 
        this.logger = CohortLogger.getLogger(Pool.class, null);

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

        cidFactory = new DistributedCohortIdentifierFactory(local, rank);
    }

    protected void setLogger(CohortLogger logger) { 
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

    public DistributedCohortIdentifierFactory getCIDFactory() { 
        return cidFactory;
    }

    public boolean isLocal(CohortIdentifier id) { 

        // TODO: think of better approach!
        if (id instanceof DistributedCohortIdentifier) { 
            DistributedCohortIdentifier tmp = (DistributedCohortIdentifier) id;
            return tmp.getIbis().equals(local);
        }

        return false;
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

        synchronized (others) { 
            if (!id.equals(local)) {
                others.add(id);
                logger.warn("JOINED " + id);
            }
        }
    }

    public void left(IbisIdentifier id) {

        synchronized (others) { 
            others.remove(id);
        }

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

    private IbisIdentifier selectRandomTarget() {

        synchronized (others) {

            int size = others.size();

            if (size == 0) { 
                return null;
            }

            return others.get(random.nextInt(size));
        }
    }

    private IbisIdentifier translate(CohortIdentifier id) { 

        if (id instanceof DistributedCohortIdentifier) { 
            return ((DistributedCohortIdentifier)id).getIbis();
        }

        return null;
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

    private boolean forward(IbisIdentifier id, Message m) { 
        return forward(id, OPCODE_MESSAGE, m);
    }

    
    public boolean forward(Message m) { 

        CohortIdentifier target = m.target;

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

    public boolean randomForward(Message m) { 

        IbisIdentifier rnd = selectRandomTarget();

        if (rnd == null) { 
            logger.warning("POOL failed to randomly select target: " +
            "no other cohorts found"); 
            return false;
        }

        SendPort s = getSendPort(rnd); 

        if (s == null) { 
            logger.warning("POOL failed to connect to " + rnd); 
            return false;
        }

        try { 
            WriteMessage wm = s.newMessage();
            wm.writeObject(m);
            wm.finish();
        } catch (Exception e) {
            logger.warning("POOL lost communication to " + rnd, e); 
            return false;
        }

        synchronized (this) {
            send++;
        }

        return true;
    }

    public CohortIdentifier selectTarget() {
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
            String tag = rm.readString();
            IbisIdentifier source = rm.origin().ibisIdentifier();
            performRegisterWithPool(source, tag);
        }
        break;
        
        case OPCODE_POOL_UPDATE_REQUEST: {
            String tag = rm.readString();
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
                SendPort s = getSendPort(tmp); 

                if (s == null) { 
                    logger.warning("POOL failed to connect to " + tmp); 
                } else { 
                    try { 
                        WriteMessage wm = s.newMessage();
                        wm.writeObject(m);
                        wm.finish();
                    } catch (Exception e) {
                        logger.warn("POOL lost communication to " + tmp, e); 
                    }
                }
            }
        }
    }

    public boolean randomForwardToPool(StealRequest sr) {

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

        return forward(id, sr);
    } 


    private void performRegisterWithPool(IbisIdentifier id, String tag) {
        
        PoolInfo tmp = null;
        
        synchronized (pools) {
            tmp = pools.get(tag);
        }

        if (tmp == null) { 
            logger.warn("Failed to find pool " + tag + " to register " + id); 
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
        forward(master, OPCODE_POOL_REGISTER_REQUEST, tag);
    }
    
    private void requestUpdate(IbisIdentifier master, String tag) {
        forward(master, OPCODE_POOL_UPDATE_REQUEST, tag);
    }
        
    public void registerWithPool(String tag) { 

        // NOTE: race condition between elect and hashmap.add!
        
        try {
            Registry reg = ibis.registry();
            IbisIdentifier id = reg.elect("STEALPOOL$" + tag);

            boolean master = id.equals(ibis.identifier()); 
            PoolInfo info = new PoolInfo(tag, id, master);
            
            synchronized (pools) {
                pools.put(tag, info);
            }
            
            if (!master) {
                performRegisterWithPool(id, tag);
                updater.addTag(tag);
            }

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
        
        synchronized (pools) {
            PoolInfo tmp = pools.get(tag);
            
            if (tmp == null) { 
                logger.warn("Received update request for unknown tag " + tag);
                System.err.println("Received update request for unknown tag " + tag);
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
