package ibis.cohort.impl.distributed.dist;

import ibis.cohort.CohortIdentifier;
import ibis.cohort.extra.Log;
import ibis.cohort.impl.distributed.ApplicationMessage;
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
import ibis.ipl.RegistryEventHandler;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class Pool implements RegistryEventHandler, MessageUpcall {
    
    private static final boolean DEBUG = true;
    
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
    
    private Log logger;
    
    private final Ibis ibis;
    private final IbisIdentifier local;
    private IbisIdentifier master;
    
    private long rank = -1;
    
    private boolean isMaster;

    private final Random random = new Random();
    
    private long received;
    private long send;
    
    public Pool(final DistributedCohort owner) throws Exception { 
    
        this.owner = owner; 
        this.logger = logger;
        
        ibis = IbisFactory.createIbis(ibisCapabilities, this, portType);

        local = ibis.identifier();

        ibis.registry().enableEvents();

        // Elect a server
        master = ibis.registry().elect("Cohort Master");

        // We determine our rank here. This rank should only be used for 
        // debugging purposes ??
        String tmp = System.getProperty("ibis.cohort.rank");
        
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
        // rp.enableMessageUpcalls();
                
        cidFactory = new DistributedCohortIdentifierFactory(local, rank);
    }

    protected void setLogger(Log logger) { 
        this.logger = logger;
        logger.info("Cohort master is " + master + " rank is " + rank);
    }
    
    public void activate() { 
        rp.enableMessageUpcalls();
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
    
    private synchronized SendPort getSendPort(IbisIdentifier id) {

        // TODO: not fault tolerant!!!

        if (id.equals(ibis.identifier())) { 
            logger.fatal("POOL Sending to myself!", new Exception());
        }
        
        SendPort sp = sendports.get(id);

        if (sp == null) { 
            try {
                sp = ibis.createSendPort(portType);
                sp.connect(id, "cohort");
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }

            sendports.put(id, sp);
        }

        return sp;
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

    public synchronized void joined(IbisIdentifier id) {
        
        if (!id.equals(local)) {
            others.add(id);
        }
    }

    public synchronized void left(IbisIdentifier id) {
        others.remove(id);
        sendports.remove(id);
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
        
        synchronized (this) {
            
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
    
    public boolean forward(Message m) { 
        
        CohortIdentifier target = m.target;
        
        if (DEBUG) { 
            logger.info("POOL FORWARD Message from " + m.source + " to " + m.target + " " + m);
        }
        
        IbisIdentifier id = translate(target);
        
        if (id == null) { 
            logger.warning("POOL failed to translate " + target 
                    + " to an IbisIdentifier");
            return false;
        }
        
        SendPort s = getSendPort(id); 
        
        if (s == null) { 
            logger.warning("POOL failed to connect to " + target); 
            return false;
        }
        
        try { 
            WriteMessage wm = s.newMessage();
            wm.writeObject(m);
            wm.finish();
        } catch (Exception e) {
            logger.warning("POOL lost communication to " + target, e); 
            return false;
        }
        
        synchronized (this) {
            send++;
        }
  
        return true;
    }
    
    public boolean randomForward(Message m) { 

        IbisIdentifier rnd = selectRandomTarget();
        
        if (rnd == null) { 
            logger.warning("POOL failed to randomly select target"); 
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

    public void upcall(ReadMessage rm) throws IOException, ClassNotFoundException {
        
        Message m = (Message) rm.readObject();
        
        synchronized (this) {
            received++;
        }
        
        if (m instanceof StealRequest) { 
            // This method may result in a nested call to send. Therefore we 
            // need to finish the ReadMessage first to allow 'stealing' of the  
            // upcall thread.
       
            if (DEBUG) { 
                logger.info("POOL RECEIVE StealRequest from " + m.source);
            }
            
            rm.finish();
            owner.deliverRemoteStealRequest((StealRequest)m);
            
        } else if (m instanceof LookupRequest) { 
            // This method may result in a nested call to send. Therefore we 
            // need to finish the ReadMessage first to allow 'stealing' of the  
            // upcall thread.
          
            if (DEBUG) { 
                logger.info("POOL RECEIVE LookupRequest from " + m.source);
            }
          
            rm.finish();
            owner.deliverRemoteLookupRequest((LookupRequest)m);
            
        } else if (m instanceof StealReply) {
            
            if (DEBUG) { 
                logger.info("POOL RECEIVE StealReply from " + m.source);
            }
            
            owner.deliverRemoteStealReply((StealReply)m);
            
        } else if (m instanceof LookupReply) { 
          
            if (DEBUG) { 
                logger.info("POOL RECEIVE LookupReply from " + m.source);
            }
            
            owner.deliverRemoteLookupReply((LookupReply)m);
        
        } else if (m instanceof ApplicationMessage) { 
          
            if (DEBUG) { 
                logger.info("POOL RECEIVE EventMessage from " + m.source);
            }
          
            owner.deliverRemoteEvent((ApplicationMessage)m);
        
        } else if (m instanceof UndeliverableEvent) { 
            
            if (DEBUG) { 
                logger.info("POOL RECEIVE UndeliverableEvent from " + m.source);
            }
            
            owner.deliverUndeliverableEvent((UndeliverableEvent)m);
        
        } else { 
            // Should never happen!
            logger.warning("POOL DROP unknown message type! " + m);
        }
    }
    
    public void broadcast(Message m) { 
        
        for (int i=0;i<others.size();i++) { 
            
            IbisIdentifier tmp = others.get(i);
            
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
                        logger.warning("POOL lost communication to " + tmp, e); 
                    }
                }
            }
        }
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