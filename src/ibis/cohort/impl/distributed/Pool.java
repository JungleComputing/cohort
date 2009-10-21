package ibis.cohort.impl.distributed;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;
import ibis.ipl.RegistryEventHandler;
import ibis.ipl.SendPort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

class Pool implements RegistryEventHandler {

    private final HashMap<IbisIdentifier, SendPort> sendports = 
        new HashMap<IbisIdentifier, SendPort>();

    private final ArrayList<IbisIdentifier> others = 
        new ArrayList<IbisIdentifier>();

    private Ibis ibis;
    private final PortType portType;       

    private IbisIdentifier local;
    private IbisIdentifier master;
    private long rank = -1;
    
    private boolean isMaster;

    private final Random random = new Random();

    Pool(PortType portType) { 
        this.portType = portType;
    }

    void setIbis(Ibis ibis) throws IOException { 

        this.ibis = ibis;

        ibis.registry().enableEvents();

        local = ibis.identifier();

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
        
        System.out.println("Cohort master is " + master + " rank is " + rank);

        isMaster = local.equals(master);
    }

    public synchronized SendPort getSendPort(IbisIdentifier id) {

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

    public void releaseSendPort(IbisIdentifier id, SendPort sp) {
        // empty
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

    public IbisIdentifier getIdentifier() {
        return local;
    }

    public long getRank() {
        return rank;
    }
    
    public boolean isMaster() {
        return isMaster;
    }
    
    public IbisIdentifier selectTarget() {
        
        synchronized (this) {
            
            int size = others.size();
            
            if (size == 0) { 
                return null;
            }
            
            return others.get(random.nextInt(size));
        }   
    }
}