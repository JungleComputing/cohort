package ibis.cohort.impl.distributed;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Cohort;
import ibis.cohort.CohortIdentifier;
import ibis.cohort.Context;
import ibis.cohort.Event;
import ibis.cohort.MessageEvent;
import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.MessageUpcall;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;

public class DistributedCohort implements Cohort, MessageUpcall {

    private static final boolean PROFILE = true;
    
    private static final byte EVENT   = 0x23;
    private static final byte STEAL   = 0x25;
    private static final byte WORK    = 0x27;
    private static final byte NO_WORK = 0x29;

    private final PortType portType = new PortType(
            PortType.COMMUNICATION_FIFO, 
            PortType.COMMUNICATION_RELIABLE, 
            PortType.SERIALIZATION_OBJECT, 
            PortType.RECEIVE_AUTO_UPCALLS,
            PortType.CONNECTION_MANY_TO_ONE);

    private static final IbisCapabilities ibisCapabilities =
        new IbisCapabilities(
                IbisCapabilities.MALLEABLE,
                IbisCapabilities.TERMINATION,
                IbisCapabilities.ELECTIONS_STRICT,                  
                IbisCapabilities.MEMBERSHIP_TOTALLY_ORDERED);

    private final Ibis ibis;
    private final IbisIdentifier local;
    private final ReceivePort rp;
    private final Pool pool;

    private final CohortIdentifier identifier;

    private long startID = 0;
    private long blockSize = 1000000;

    private int cohortCount = 0;

    private boolean pendingSteal = false;
    
    private MultiThreadedCohort mt;

    private Context context;
    
    private List<GarbageCollectorMXBean> gcbeans; 
      
    private long messagesSend;
    private long eventsSend;
    private long stealsSend;
    private long workSend;
    private long no_workSend;

    private long messagesReceived;
    private long eventsReceived;
    private long stealsReceived;
    private long workReceived;
    private long no_workReceived;
    
    public DistributedCohort() throws Exception {         

        if (PROFILE) { 
           gcbeans = ManagementFactory.getGarbageCollectorMXBeans();
        }
        
        context = Context.ANY;
        
        // Init Ibis here...
        pool = new Pool(portType);

        ibis = IbisFactory.createIbis(ibisCapabilities, pool, portType);

        pool.setIbis(ibis);

        rp = ibis.createReceivePort(portType, "cohort", this);

        local = ibis.identifier();

        identifier = getCohortIdentifier();

        mt = new MultiThreadedCohort(this, getCohortIdentifier(), 0);        

        rp.enableConnections();
        rp.enableMessageUpcalls();

    }

    public synchronized DistributedActivityIdentifierGenerator getIDGenerator(
            CohortIdentifier identifier) {

        DistributedActivityIdentifierGenerator tmp = 
            new DistributedActivityIdentifierGenerator(
                    (DistributedCohortIdentifier) identifier,  startID, 
                    startID+blockSize);

        startID += blockSize;
        return tmp;
    }

    public void done() {
        
        try { 
            // NOTE: this will proceed directly on the master. On other instances, 
            // it blocks until the master terminates. 
            pool.terminate();
        } catch (Exception e) {
            System.err.println("Failed to terminate pool!" + e);
            e.printStackTrace(System.err);            
        }
        
        mt.done();        
        
        printStatistics();
        
        pool.cleanup();
    }

    private void printStatistics() { 
        
        synchronized (System.out) {
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
            
            if (PROFILE) { 

                System.out.println("GC beans     : " + gcbeans.size());

                for (GarbageCollectorMXBean gc : gcbeans) { 
                    System.out.println(" GC bean : " + gc.getName());
                    System.out.println("   count : " + gc.getCollectionCount());
                    System.out.println("   time  : " + gc.getCollectionTime());
                }
            }
        }
    }
    
    public void send(ActivityIdentifier source, ActivityIdentifier target, 
            Object o) {
        forwardEvent(new MessageEvent(source, target, o));
    }

    public ActivityIdentifier submit(Activity job) {

        // System.out.println("DIST submit");

        return mt.submit(job);
    }

    private void forwardObject(IbisIdentifier id, byte opcode, Object data) { 

        SendPort sp = pool.getSendPort(id);         

        if (sp == null) { 
            // TODO: decent error handling
            System.err.println("EEP: failed to forward object!"); 
            return;
        }
        
        try { 
            WriteMessage wm = sp.newMessage();

            wm.writeByte(opcode);
            wm.writeObject(data);
            wm.finish();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        synchronized (this) {
            messagesSend++;
        }
        
        pool.releaseSendPort(id, sp);        
    }
    
    private void forwardOpcode(IbisIdentifier id, byte opcode) { 

        SendPort sp = pool.getSendPort(id);         

        if (sp == null) { 
            // TODO: decent error handling
            System.err.println("EEP: failed to forward object!"); 
            return;
        }
        
        try { 
            WriteMessage wm = sp.newMessage();

            wm.writeByte(opcode);
            wm.finish();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        synchronized (this) {
            messagesSend++;
        }
        
        pool.releaseSendPort(id, sp);        
    }
    
    private void forwardObject(DistributedCohortIdentifier cid, byte opcode, 
            Object data) { 
            forwardObject(cid.getIbis(), opcode, data);
    }

    private boolean isLocal(DistributedCohortIdentifier id) { 
        return local.equals(id.getIbis());
    }

    void forwardEvent(Event e) {

        DistributedCohortIdentifier id = 
            ((DistributedActivityIdentifier) e.target).getLastKnownCohort();
        
        if (isLocal(id)) { 
            mt.deliverEvent(e);
        } else {
            forwardObject(id, EVENT, e);
            
            synchronized (this) {
                eventsSend++;
            }
        }
    }

    private ActivityRecord stealRequest(IbisIdentifier src, Context c) { 
    
        // TODO: improve
        return mt.stealRequest(null);
    }

    
    void postStealRequest(IbisIdentifier src, Context c) { 
        // TODO: implement!
    }
    
    private synchronized boolean setPendingSteal(boolean value) { 
        boolean tmp = pendingSteal; 
        pendingSteal = value;
        return tmp;
    }
    
    ActivityRecord stealAttempt(CohortIdentifier source) {
        
        boolean pending = setPendingSteal(true);
    
        if (pending) { 
            // Steal request was already pending, so ignore this one.
            return null;
        }
        
        // Find some other cohort and send it a steal request.
        IbisIdentifier id = pool.selectTarget();

        if (id != null) { 
            
        //    System.out.println("Sending STEAL from " + local + " to " + id);
            
            forwardObject(id, STEAL, getContext());
            
            synchronized (this) {
                stealsSend++;
            }
        }
       
        return null;
    }

    void deliverEvent(Event e) { 
        mt.deliverEvent(e);
    }
    
    void addActivityRecord(ActivityRecord record) {
        mt.addActivityRecord(record, false);
    }
    
    public void upcall(ReadMessage rm) 
        throws IOException, ClassNotFoundException {

        byte opcode = rm.readByte();
        
        switch (opcode) { 
        case EVENT:
            
        //    System.out.println("Received EVENT from " + rm.origin().ibisIdentifier() + " on " + local);
            
            Event e = (Event) rm.readObject();
            mt.deliverEvent(e);
            
            synchronized (this) {
                messagesReceived++;
                eventsReceived++;
            }
            
            break;
        case STEAL:
            
          //  System.out.println("Received STEAL from " + rm.origin().ibisIdentifier() + " on " + local);
            
            IbisIdentifier src = rm.origin().ibisIdentifier();
            Context c = (Context) rm.readObject();

            // Finish the message, since we need to communicate here!
            rm.finish();
            
            ActivityRecord tmp = stealRequest(src, c);
            
            if (tmp != null) { 
         //       System.out.println("Sending WORK from " + local + " to " + src);
                
                forwardObject(src, WORK, tmp);
                
                synchronized (this) {
                    workSend++;
                }
            } else { 
                
           //     System.out.println("Sending NO_WORK from " + local + " to " + src);
                    
                forwardOpcode(src, NO_WORK);                  
            
                synchronized (this) {
                    no_workSend++;
                }
            }

            synchronized (this) {
                messagesReceived++;
                stealsReceived++;
            }
            
            break;
            
        case WORK: 
            
         //   System.out.println("Received WORK from " + rm.origin().ibisIdentifier() + " on " + local);
                        
            boolean ispending = setPendingSteal(false);
            
            if (!ispending) { 
                System.err.println("Received stray WORK!");
            }
            
            ActivityRecord a = (ActivityRecord) rm.readObject();
            mt.addActivityRecord(a, false);
          
            synchronized (this) {
                messagesReceived++;
                workReceived++;
            }
          
            break;
            
        case NO_WORK:
            
        //    System.out.println("Received NO_WORK from " + rm.origin().ibisIdentifier() + " on " + local);
            
            boolean pending = setPendingSteal(false);
            
            if (!pending) { 
                System.err.println("Received stray NO_WORK!");
            }
          
            synchronized (this) {
                messagesReceived++;
                no_workReceived++;
            }
            
            break;
            
        default:
            throw new IOException("Unknown opcode: " + opcode);
        }
    }

    public synchronized CohortIdentifier getCohortIdentifier() {
        return new DistributedCohortIdentifier(local, cohortCount++);
    }

    public CohortIdentifier identifier() {
        return identifier;
    }

    public void cancel(ActivityIdentifier activity) {
        mt.cancel(activity);
    }

    public boolean isMaster() { 
        return pool.isMaster();
    }

    public synchronized Context getContext() {
        return context;
    }

    public synchronized void setContext(Context context) {
        // TODO: implement
    }
}
