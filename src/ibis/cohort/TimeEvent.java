package ibis.cohort;

public class TimeEvent extends Event {

    public final Time time;

    public TimeEvent(Identifier source, Time time) {
        super(source);
        this.time = time;
    }

}
