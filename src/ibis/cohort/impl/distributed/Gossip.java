package ibis.cohort.impl.distributed;

import ibis.cohort.CohortIdentifier;
import ibis.cohort.Context;

import java.io.Serializable;

public class Gossip implements Serializable {

    private static final long serialVersionUID = 2068820337089838573L;

    public final CohortIdentifier id;
    public final Context context;
    public final long timestamp;
  
    public Gossip(final CohortIdentifier id, final Context context, 
            final long timestamp) {
       
        super();
        this.id = id;
        this.context = context;
        this.timestamp = timestamp;
    }
}
