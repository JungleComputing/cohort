package ibis.constellation.impl.dist;

import ibis.ipl.IbisIdentifier;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Random;

public class PoolInfo implements Serializable {

    final String tag;
    
    final IbisIdentifier master;
    final boolean isMaster;
    
    long timestamp; 
    ArrayList<IbisIdentifier> members;
    
    PoolInfo(String tag, IbisIdentifier master, boolean isMaster) { 
        this.tag = tag;
        this.master = master;
        this.isMaster = isMaster;
        members = new ArrayList<IbisIdentifier>();
        members.add(master);
        timestamp = 1;
    }
    
    synchronized void addMember(IbisIdentifier id) {
        members.add(id);
        timestamp++;
    }                   
    
    synchronized  void removeMember(IbisIdentifier id) { 
        members.remove(id);
        timestamp++;
    }
    
    synchronized long currentTimeStamp() { 
        return timestamp;
    }
    
    synchronized IbisIdentifier selectRandom(Random random) { 
        return members.get(random.nextInt(members.size()));
    }    
}
