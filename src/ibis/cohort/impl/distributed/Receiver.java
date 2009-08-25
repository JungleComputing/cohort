package ibis.cohort.impl.distributed;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;

import ibis.cohort.CohortIdentifier;
import ibis.cohort.Context;
import ibis.cohort.Event;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.ReceiveTimedOutException;

class Receiver extends Thread {

    private static final byte EVENT   = 0x23;
    private static final byte STEAL   = 0x25;
    private static final byte REPLY   = 0x27;
    
    private static final int STEAL_TIMEOUT = 1000;
        
    private final ReceivePort rp;
    private final DistributedCohort parent;

    private long messagesReceived;
    private long eventsReceived;
    private long stealsReceived;
    private long workReceived;
    private long no_workReceived;

    private boolean done = false;

    private String stats;
    
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
                StealRequest r = (StealRequest) rm.readObject();
                r.setTimeout(System.currentTimeMillis() + STEAL_TIMEOUT);
                r.setLocal(false);
                parent.postStealRequest(r);
                messagesReceived++;
                stealsReceived++;
                break;

            case REPLY: 
                StealReply reply = (StealReply) rm.readObject();
                parent.incomingStealReply(reply);      
                messagesReceived++;
            
                if (reply.work == null) { 
                    no_workReceived++;
                } else { 
                    workReceived++;
                }
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

    public synchronized String waitForStatistics() {
        
        while (stats == null) { 
            try { 
                wait();
            } catch (Exception e) {
                // ignore
            }
        } 
        
        return stats;
    }

    public synchronized void setStats(String s) { 
        stats = s;
        notifyAll();
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
        
        // TODO: bit of a mess....
        StringBuilder tmp = new StringBuilder();

        tmp.append("Messages received : " + messagesReceived + "\n");
        tmp.append("           Events : " + eventsReceived + "\n");
        tmp.append("           Steals : " + stealsReceived + "\n");
        tmp.append("             Work : " + workReceived + "\n");
        tmp.append("          No work : " + no_workReceived + "\n");
        
        setStats(tmp.toString());
    }
}
