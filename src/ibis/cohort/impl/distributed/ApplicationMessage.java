package ibis.cohort.impl.distributed;

import ibis.cohort.ActivityIdentifier;
import ibis.cohort.CohortIdentifier;
import ibis.cohort.Event;

public class ApplicationMessage extends Message {

    private static final long serialVersionUID = -5430024744123215066L;

    public final Event event;
    
    public ApplicationMessage(final CohortIdentifier source, final Event e) { 
        super(source);
        this.event = e;
    }
    
    public boolean requiresLookup() {
        return true;
    }

    public ActivityIdentifier targetActivity() {
        return event.target;
    }       
}
