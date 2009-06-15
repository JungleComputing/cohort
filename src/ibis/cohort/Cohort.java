package ibis.cohort;

public interface Cohort {

    ActivityIdentifier submit(Activity job);
    
    void send(ActivityIdentifier source, ActivityIdentifier target, Object o);    

   // void finished(Activity id, Object result);
   // void unsuspend(Activity id);

    void cancel(ActivityIdentifier activity);
    void cancelAll();
    void done();

}
