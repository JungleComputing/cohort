package ibis.cohort.extra;

import ibis.cohort.Context;
import ibis.cohort.Activity;

public abstract class AtomicJob extends Activity {

	private static final long serialVersionUID = -5751438180590424232L;

	protected AtomicJob(Context location) {
		super(location);
	}
	
}
