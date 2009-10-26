package ibis.cohort.impl.distributed;

import ibis.cohort.Event;
import ibis.ipl.MessageUpcall;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
//import ibis.ipl.ReceiveTimedOutException;

import java.io.IOException;

class Receiver implements MessageUpcall { 

    private static final byte EVENT   = 0x23;
    private static final byte STEAL   = 0x25;
    private static final byte REPLY   = 0x27;
    
    private final DistributedCohort parent;

    private long messagesReceived;
    private long eventsReceived;
    private long stealsReceived;
    private long workReceived;
    private long no_workReceived;

    private boolean done = false;

    private String stats;
    
    Receiver(DistributedCohort parent) { 
        this.parent = parent;
        //setName("Receiver");
    }

    /*
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
                parent.incomingRemoteStealRequest(r);
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
    }*/

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

    /*
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
    }*/
    
    public void upcall(ReadMessage rm) throws IOException, ClassNotFoundException {
        
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

        case REPLY: 
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
        }
    }
}
