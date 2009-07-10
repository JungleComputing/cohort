package ibis.cohort.impl.distributed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

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

public class DistributedCohort implements Cohort, MessageUpcall {

    private static final byte EVENT = 0x23;
    private static final byte STEAL = 0x25;
    private static final byte WORK  = 0x27;

    private final PortType portType = new PortType(
            PortType.COMMUNICATION_FIFO, 
            PortType.COMMUNICATION_RELIABLE, 
            PortType.SERIALIZATION_OBJECT, 
            PortType.RECEIVE_AUTO_UPCALLS,
            PortType.CONNECTION_MANY_TO_ONE);

    private static final IbisCapabilities ibisCapabilities =
        new IbisCapabilities(
                IbisCapabilities.MALLEABLE, 
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

    private MultiThreadedCohort mt;

    private Context context;
    
    public DistributedCohort() throws Exception {         

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
        mt.done();        
        pool.done();
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
            ((DistributedActivityIdentifier) e.target).getCohort();

        if (isLocal(id)) { 
            mt.deliverEvent(e);
        } else {
            forwardObject(id, EVENT, e);
        }
    }

    private ActivityRecord stealRequest(IbisIdentifier src, Context c) { 
    
        // TODO: improve
        return mt.stealRequest(null);
    }

    ActivityRecord stealAttempt(CohortIdentifier source) {

        // Find some other cohort and send it a steal request.
        IbisIdentifier id = pool.selectTarget();

        if (id != null) { 
            
            System.out.println("Sending STEAL from " + local + " to " + id);
            
            forwardObject(id, STEAL, getContext());
        }
       
        return null;
    }

    public void upcall(ReadMessage rm) 
    throws IOException, ClassNotFoundException {

        byte opcode = rm.readByte();
        
        switch (opcode) { 
        case EVENT:
            Event e = (Event) rm.readObject();
            mt.deliverEvent(e);
            break;
        case STEAL:
            IbisIdentifier src = rm.origin().ibisIdentifier();
            Context c = (Context) rm.readObject();

            ActivityRecord tmp = stealRequest(src, c);
            
            if (tmp != null) { 
                
                // Finish the message, since we may need to communicate here!
                rm.finish();
                
                System.out.println("Sending WORK from " + local + " to " + src);
                
                forwardObject(src, WORK, tmp);
            }

            break;    
        case WORK:
            ActivityRecord a = (ActivityRecord) rm.readObject();
            mt.addActivityRecord(a, false);
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
