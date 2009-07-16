package ibis.cohort;

import java.io.Serializable;

public abstract class Event implements Serializable {

    public final ActivityIdentifier source;
    public final ActivityIdentifier target;
    
    public Event(ActivityIdentifier source, ActivityIdentifier target) { 
        this.source = source;
        this.target = target;
    }
}
