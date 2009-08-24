package ibis.cohort.impl.distributed;

import ibis.cohort.Context;
import ibis.ipl.IbisIdentifier;

class StealRequest {
    
    final IbisIdentifier src;
    final Context context;
    final long timeout;
    
    public StealRequest(final IbisIdentifier src, final Context context, 
            final long timeout) {
        super();
        this.src = src;
        this.context = context;
        this.timeout = timeout;
    }
}
