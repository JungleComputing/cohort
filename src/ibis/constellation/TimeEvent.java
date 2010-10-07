package ibis.constellation;

public class TimeEvent extends Event {

    public final Time time;

    public TimeEvent(ActivityIdentifier target, Time time) {
        super(null, target);
        this.time = time;
    }

}
