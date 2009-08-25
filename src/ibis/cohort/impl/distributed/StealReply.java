package ibis.cohort.impl.distributed;

import java.io.Serializable;

import ibis.cohort.CohortIdentifier;

class StealReply implements Serializable {
    
    private static final long serialVersionUID = 2655647847327367590L;
   
    public final CohortIdentifier target;
    public final ActivityRecord work;
    
    public StealReply(final CohortIdentifier target, final ActivityRecord work) {  
        super();
        this.target = target;
        this.work = work;
    }
}
