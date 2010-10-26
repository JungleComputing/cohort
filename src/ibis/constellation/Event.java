package ibis.constellation;

import java.io.Serializable;

public abstract class Event implements Serializable {

    private static final long serialVersionUID = 8672434537078611592L;
	
    public final ActivityIdentifier source;
    public final ActivityIdentifier target;
    
    public Event(ActivityIdentifier source, ActivityIdentifier target) { 
        this.source = source;
        this.target = target;
    }
}
