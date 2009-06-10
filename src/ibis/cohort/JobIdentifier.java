package ibis.cohort;

public class JobIdentifier {

	private static long nextID = 0;
	
	private final long id;
	
	private JobIdentifier(long id) { 
		this.id = id;
	}
	
	public static synchronized JobIdentifier getNext() {
		return new JobIdentifier(nextID++); 
	}

	public String toString() { 
		return "Job-" + id;
	}
	
}
