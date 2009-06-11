package ibis.cohort;

public abstract class Event {

	public final JobIdentifier source;

	public Event(JobIdentifier source) { 
		this.source = source;
	}
}
