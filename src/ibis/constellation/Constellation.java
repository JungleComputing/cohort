package ibis.constellation;

public interface Constellation {
    
    public boolean isMaster();

    public ConstellationIdentifier identifier();
    
    public boolean activate();
    
    public void done();


    
    public WorkerContext getContext();

    
    
    public ActivityIdentifier submit(Activity job);
    
    public void cancel(ActivityIdentifier activity);
    
    
    
    public void send(ActivityIdentifier source, ActivityIdentifier target, Object o);    

    public void send(Event e);    
    
        
    
    
    
    // TODO: change this to use Pool as scope instead of Context!
  //  public boolean register(String name, ActivityIdentifier id, ActivityContext scope);
  //  public ActivityIdentifier lookup(String name,  ActivityContext scope);
  //  public boolean deregister(String name,  ActivityContext scope);
    
    /*
    public boolean register(String name, ActivityIdentifier id, StealPool scope);
    public ActivityIdentifier lookup(String name,  StealPool scope);
    public boolean deregister(String name,  StealPool scope);
    */
    
    
    /*
    public void setContext(WorkerContext context) throws Exception;
    public void setContext(ConstellationIdentifier id, WorkerContext context) throws Exception;
    */
    
  //  public void addContext(Context ... contexts);
    
  //  public void removeContext(Context ...contexts);
    
  //  public void clearContext();

    
    /*
    public Constellation [] getSubCohorts();
    
    public ConstellationIdentifier [] getLeafIDs();
    */
    
    
}
