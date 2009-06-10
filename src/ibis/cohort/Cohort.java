package ibis.cohort;

public interface Cohort {

	// Async call
	JobIdentifier submit(Job job, ResultHandler h);

	// Sync call
	Object submit(Job job);

        void finished(Job job, Object result);
        void unsuspend(Job job);
        
	boolean cancel(JobIdentifier job);
	boolean cancelAll();
	void done();

        
        
}
