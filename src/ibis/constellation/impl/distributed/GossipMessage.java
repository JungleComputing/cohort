package ibis.constellation.impl.distributed;

import java.io.Serializable;

import ibis.constellation.CohortIdentifier;

public class GossipMessage extends Message {

    private static final long serialVersionUID = -2735555540658194683L;
  
    private Gossip [] gossip;
    
    public GossipMessage(CohortIdentifier source, CohortIdentifier target, Gossip [] gossip) {
        super(source, target);
        this.gossip = gossip;
    }

    public Gossip [] getGossip() { 
        return gossip;
    }
}
