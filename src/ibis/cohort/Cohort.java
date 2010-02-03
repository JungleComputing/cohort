package ibis.cohort;

public interface Cohort {
    
    public boolean isMaster();

    public CohortIdentifier identifier();
    
    public ActivityIdentifier submit(Activity job);
    
    public void cancel(ActivityIdentifier activity);
    
    
    public void send(ActivityIdentifier source, ActivityIdentifier target, Object o);    

    public void send(Event e);    
    
    public boolean register(String name, ActivityIdentifier id, Context scope);

    public ActivityIdentifier lookup(String name, Context scope);
    
    public boolean deregister(String name, Context scope);

    
    public void done();
    
    public Context getContext();
    
    public void setContext(Context context) throws Exception;
    public void setContext(CohortIdentifier id, Context context) throws Exception;
    
  //  public void addContext(Context ... contexts);
    
  //  public void removeContext(Context ...contexts);
    
  //  public void clearContext();
    
    public Cohort [] getSubCohorts();
    
    public boolean activate();
    
    
    
}
