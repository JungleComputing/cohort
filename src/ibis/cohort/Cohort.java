package ibis.cohort;

public interface Cohort {

	// Async call
	JobIdentifier submit(Job job, ResultHandler h);

	// Sync call
	Object submit(Job job);

	void send(JobIdentifier source, JobIdentifier target, Object o);    
    
    void finished(Job id, Object result);
    void unsuspend(Job id);
        
	boolean cancel(JobIdentifier job);
	boolean cancelAll();
	void done();

        
        
}
