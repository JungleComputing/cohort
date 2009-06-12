package ibis.cohort;

public abstract class Event {

    public final Identifier source;

    public Event(Identifier source) { 
        this.source = source;
    }
}
