package ibis.cohort;

public class MessageEvent extends Event {

	public final Object message;
	
	public MessageEvent(JobIdentifier source, Object message) {
		super(source);
		this.message = message;
	}

}
