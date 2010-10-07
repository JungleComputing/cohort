package ibis.constellation.impl.distributed;

import ibis.constellation.CohortIdentifier;
import ibis.constellation.Event;

public class UndeliverableEvent extends Message {

    private static final long serialVersionUID = 2006268088130257399L;
    
    public final Event event;
    
    public UndeliverableEvent(
            final CohortIdentifier source, 
            final CohortIdentifier target, 
            final Event event) {
        
        super(source, target);
        this.event = event;
    }

    
}
