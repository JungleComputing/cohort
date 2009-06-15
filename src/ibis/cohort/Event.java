package ibis.cohort;

public abstract class Event {

    public final ActivityIdentifier source;
    public final ActivityIdentifier target;
    
    public Event(ActivityIdentifier source, ActivityIdentifier target) { 
        this.source = source;
        this.target = target;
    }
}
