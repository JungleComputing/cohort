package ibis.cohort;

public abstract class SimpleActivity extends Activity {
    
    protected ActivityIdentifier parent;
    
    protected SimpleActivity(ActivityIdentifier parent, ActivityContext context) {
        this(parent, context, false);
    }

    protected SimpleActivity(ActivityIdentifier parent, ActivityContext context, 
            boolean restictToLocal) {
        super(context, restictToLocal);
        this.parent = parent;
    }
    
    @Override
    public void initialize() throws Exception {
        simpleActivity();
        finish();
    }
    
    @Override
    public void cancel() throws Exception {
        // not used
    }

    @Override
    public void cleanup() throws Exception {
        // not used
    }
    
    @Override
    public void process(Event e) throws Exception {
        // not used
    }
    
    public abstract void simpleActivity() throws Exception;   

    public ActivityIdentifier getParent() { 
        return parent;
    }
    
    public void setParent(ActivityIdentifier parent) { 
        this.parent = parent;
    }
}
