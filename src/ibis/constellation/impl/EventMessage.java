package ibis.constellation.impl;

import ibis.constellation.ConstellationIdentifier;
import ibis.constellation.Event;

public class EventMessage extends Message {

    private static final long serialVersionUID = -5430024744123215066L;

    public final Event event;
    
    public EventMessage(final ConstellationIdentifier source, final ConstellationIdentifier target, final Event e) { 
        super(source, target);
        this.event = e;
    }    
}
