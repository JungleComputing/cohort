package ibis.cohort;

public abstract class SimpleActivity extends Activity {
    
    protected SimpleActivity(Context context) {
        super(context);
    }

    @Override
    public void initialize() throws Exception {
        simpleActivity();
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
}
