package ibis.constellation;

import java.io.Serializable;

public class Event implements Serializable {

    private static final long serialVersionUID = 8672434537078611592L;
	
    public final ActivityIdentifier source;
    public final ActivityIdentifier target;
    
    public final Object data;

    public Event(ActivityIdentifier source, ActivityIdentifier target, Object data) { 
    	this.source = source;
        this.target = target;
        this.data = data;
    }
}
