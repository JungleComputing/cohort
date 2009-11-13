package ibis.cohort;

public interface Cohort {
    
    public boolean isMaster();

    public CohortIdentifier identifier();
    
    public ActivityIdentifier submit(Activity job);
    
    public void send(ActivityIdentifier source, ActivityIdentifier target, Object o);    

    public void cancel(ActivityIdentifier activity);
   
    public void done();
    
    public Context getContext();
    
    public void setContext(Context context);
    
    public Cohort [] getSubCohorts();
    
    public boolean activate();
}
