package test.spawntest;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Event;
import ibis.cohort.context.UnitActivityContext;

public class Dummy extends Activity {

    private static final long serialVersionUID = 5970093414747228592L;
    
    private final ActivityIdentifier parent;
    
    public Dummy(ActivityIdentifier parent) {
        super(UnitActivityContext.DEFAULT);
        this.parent = parent;
    }

    @Override
    public void initialize() throws Exception {
        cohort.send(identifier(), parent, null);
        finish();
    }

    @Override
    public void process(Event e) throws Exception {
        // unused
    }

    @Override
    public void cleanup() throws Exception {
        // unused
    }

    @Override
    public void cancel() throws Exception {
        // TODO Auto-generated method stub
        
    }

}
