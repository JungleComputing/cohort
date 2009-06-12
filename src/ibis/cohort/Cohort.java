package ibis.cohort;

public interface Cohort {

    Identifier submit(Activity job);
    
    void send(Identifier source, Identifier target, Object o);    

   // void finished(Activity id, Object result);
   // void unsuspend(Activity id);

    boolean cancel(Identifier activity);
    boolean cancelAll();
    void done();

}
