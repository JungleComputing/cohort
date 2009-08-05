package ibis.cohort.impl.distributed;

import java.io.IOException;

import ibis.cohort.Context;
import ibis.cohort.Event;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.ReceiveTimedOutException;

class Receiver extends Thread {

    private static final byte EVENT   = 0x23;
    private static final byte STEAL   = 0x25;
    private static final byte WORK    = 0x27;
    private static final byte NO_WORK = 0x29;

    private final ReceivePort rp;
    private final DistributedCohort parent;

    private long messagesReceived;
    private long eventsReceived;
    private long stealsReceived;
    private long workReceived;
    private long no_workReceived;

    private boolean done = false;

    Receiver(ReceivePort rp, DistributedCohort parent) { 
        this.rp = rp;
        this.parent = parent;
        setName("Receiver");
    }

    void handleMessage(ReadMessage rm) {

        try { 
            byte opcode = rm.readByte();

            switch (opcode) { 
            case EVENT:
                Event e = (Event) rm.readObject();
                parent.deliverEvent(e);
                messagesReceived++;
                eventsReceived++;        
                break;

            case STEAL:
                IbisIdentifier src = rm.origin().ibisIdentifier();
                Context c = (Context) rm.readObject();
                parent.postStealRequest(src, c);
                messagesReceived++;
                stealsReceived++;
                break;

            case WORK: 
                ActivityRecord a = (ActivityRecord) rm.readObject();
                parent.addActivityRecord(a);      
                messagesReceived++;
                workReceived++;
                break;

            case NO_WORK:
                messagesReceived++;
                no_workReceived++;
                break;

            default:
                throw new IOException("Unknown opcode: " + opcode);
            }

            rm.finish();        
        } catch (ClassNotFoundException e) {
            rm.finish(new IOException("Class not found", e));
        } catch (IOException e) {
            rm.finish(e);
        }
    }

    public synchronized void done() { 
        done = true;
    }

    private synchronized boolean getDone() { 
        return done;
    }


    public void run() { 

        while (!getDone()) {
            ReadMessage rm = null;
            
            try {
                rm = rp.receive(1000);
            } catch (ReceiveTimedOutException e) {
                // allowed
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (rm != null) { 
                handleMessage(rm);
            }
        }
    }
}
