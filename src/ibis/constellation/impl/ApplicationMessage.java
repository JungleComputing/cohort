package ibis.constellation.impl;

import ibis.constellation.ActivityIdentifier;
import ibis.constellation.ConstellationIdentifier;
import ibis.constellation.Event;

public class ApplicationMessage extends Message {

    private static final long serialVersionUID = -5430024744123215066L;

    public final Event event;
    
    public ApplicationMessage(final ConstellationIdentifier source, final Event e) { 
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
