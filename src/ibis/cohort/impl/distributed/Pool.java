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

    private static final int STEAL_TIMEOUT = 1000; 

    private final HashMap<IbisIdentifier, SendPort> sendports = 
        new HashMap<IbisIdentifier, SendPort>();

    private final ArrayList<IbisIdentifier> others = 
        new ArrayList<IbisIdentifier>();

    private Ibis ibis;
    private final PortType portType;       

    private IbisIdentifier local;
    private IbisIdentifier master;

    private boolean isMaster;

    private final Random random = new Random();

    private long nextSteal = 0; 

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

    public synchronized void joined(IbisIdentifier id) {
        others.add(id);
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

    public boolean isMaster() {
        return isMaster;
    }

    public IbisIdentifier selectTarget() {

        long time = System.currentTimeMillis();

        if (time < nextSteal) { 
            return null;
        } 
        
        nextSteal = time + STEAL_TIMEOUT;

        synchronized (this) {
            final int size = others.size();

            IbisIdentifier tmp = others.get(random.nextInt(size));

            while (local.equals(tmp)) {

                if (size == 1) { 
                    return null;
                }

                tmp = others.get(random.nextInt(size));
            }

            return tmp;
        }   
    }
}