package ibis.cohort.impl.distributed;

import ibis.cohort.Context;

public class LocalStealRequest {
    
    final int workerID;
    final Context context;
    
    public LocalStealRequest(final int workerID, final Context context) {
        super();
        this.workerID = workerID;
        this.context = context;
    }
    
    
}
