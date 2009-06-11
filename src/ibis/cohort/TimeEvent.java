package ibis.cohort;

public class TimeEvent extends Event {

	public final Time time;
	
	public TimeEvent(JobIdentifier source, Time time) {
		super(source);
		this.time = time;
	}

}
