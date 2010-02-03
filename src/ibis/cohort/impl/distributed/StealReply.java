package ibis.cohort.impl.distributed;

import ibis.cohort.CohortIdentifier;

public class StealReply extends Message {
    
    private static final long serialVersionUID = 2655647847327367590L;
   
    public final ActivityRecord work;
    
    public StealReply(
            final CohortIdentifier source, 
            final CohortIdentifier target, 
            final ActivityRecord work) { 
        super(source, target);
        this.work = work;
    }
}
