package ibis.cohort.impl.distributed;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.MessageUpcall;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.RegistryEventHandler;
import ibis.ipl.SendPort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

class Pool implements RegistryEventHandler, MessageUpcall {
  
    private final HashMap<IbisIdentifier, SendPort> sendports = 
        new HashMap<IbisIdentifier, SendPort>();
    
    private final ArrayList<IbisIdentifier> others = 
        new ArrayList<IbisIdentifier>();
    
    private Ibis ibis;
    private final PortType portType;       
    
    private IbisIdentifier local;
    private IbisIdentifier master;
    
    private boolean isMaster;
    
    Pool(PortType portType) { 
        this.portType = portType;
    }
    
    void setIbis(Ibis ibis) throws IOException { 
        
        this.ibis = ibis;

        ibis.registry().enableEvents();

        local = ibis.identifier();
        
        // Elect a server
        master = ibis.registry().elect("Cohort Master");

        System.out.println("Cohort master is " + master);
    
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
    
    public void done() { 
        try {
            ibis.end();
        } catch (IOException e) {
            e.printStackTrace();
        }        
    }

    public IbisIdentifier getIdentifier() {
        return local;
    }

    public void upcall(ReadMessage arg0) throws IOException, ClassNotFoundException {
        // TODO Auto-generated method stub
        
    }

    public boolean isMaster() {
        return isMaster;
    }   
}
