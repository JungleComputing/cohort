package ibis.cohort.impl.distributed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Cohort;
import ibis.cohort.Event;
import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.MessageUpcall;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.RegistryEventHandler;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

public class DistributedCohort implements Cohort, MessageUpcall, RegistryEventHandler {

    private static final byte MESSAGE = 0x23;
    private static final byte STEAL   = 0x25;
    private static final byte REPLY   = 0x27;
    
    private Ibis ibis;
    private IbisIdentifier local;
    private PortType portType;
    
    private final HashMap<IbisIdentifier, SendPort> sendports = 
        new HashMap<IbisIdentifier, SendPort>();
    
    private final ArrayList<IbisIdentifier> others = 
        new ArrayList<IbisIdentifier>();
    
    private ReceivePort rp;
    
    private long startID = 0;
    private long blockSize = 1000000;

    private MultiThreadedCohort mt;
          
    public DistributedCohort() {         
        
        // Init Ibis here...
        
        mt = new MultiThreadedCohort(this, 0);        
    }

    public synchronized IDGenerator getIDGenerator(int workerID) {
        IDGenerator tmp = new IDGenerator(ibis.identifier(), workerID, 
                startID, startID+blockSize);
        startID += blockSize;
        return tmp;
    }

    
    public void cancel(ActivityIdentifier activity) {
        // TODO Auto-generated method stub
        
    }

    public void cancelAll() {
        // TODO Auto-generated method stub
        
    }

    public void done() {

        mt.done();        
        
        try {
            ibis.end();
        } catch (IOException e) {
            e.printStackTrace();
        }        
    }

    private synchronized SendPort getSendPort(IbisIdentifier id) {
        
        // TODO: not fault tolerant!!!
        
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

    private void releaseSendPort(IbisIdentifier id, SendPort sp) {
        // empty
    }
    
    private void sendRemote(IbisIdentifier id, ActivityIdentifier source, 
            ActivityIdentifier target, Object o) {
        
        SendPort sp = getSendPort(id);         
        
        try { 
            WriteMessage wm = sp.newMessage();

            wm.writeByte(MESSAGE);
            wm.writeObject(source);
            wm.writeObject(target);
            wm.writeObject(o);
            wm.finish();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        releaseSendPort(id, sp);        
    }
        
    public void send(ActivityIdentifier source, ActivityIdentifier target, Object o) {
    
        IbisIdentifier id = ((Identifier) target).getIbis();
        
        if (local.equals(id)) { 
            mt.send(source, target, o);
        } else { 
            sendRemote(id, source, target, o);
        }         
    }

    public ActivityIdentifier submit(Activity job) {
        return mt.submit(job);
    }
    
    void sendEvent(Event e) {
        
    }
    
    void forwardEvent(Event e) {
    
        IbisIdentifier id = ((Identifier) e.target).getIbis();
        
        if (id.equals(local)) { 
            mt.deliverEvent(e);
        } else {
            sendEvent(e);
        }
    }
    
    
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
        others.add(id);
    }

    public void left(IbisIdentifier id) {
        others.remove(id);
        sendports.remove(id);
    }

    public void poolClosed() {
        // ignored
    }

    public void poolTerminated(IbisIdentifier id) {
        // ignored
    }        

    public void upcall(ReadMessage rm) throws IOException, ClassNotFoundException {
        
        byte opcode = rm.readByte();
        
        switch (opcode) { 
        case MESSAGE:

            
            
        case STEAL:
            
        case REPLY:
            
        }
    } 
    
}
