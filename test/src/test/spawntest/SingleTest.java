package test.spawntest;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Event;
import ibis.cohort.context.UnitActivityContext;

public class SingleTest extends Activity {

    private static final long serialVersionUID = 5970093414747228592L;
    
    private final ActivityIdentifier parent;
    
    private final int spawns;
    private int replies;
    
    public SingleTest(ActivityIdentifier parent, int spawns) {
        super(UnitActivityContext.DEFAULT);
        this.parent = parent;
        this.spawns= spawns;
    }

    @Override
    public void initialize() throws Exception {
        for (int i=0;i<spawns;i++) { 
            executor.submit(new Dummy(identifier()));
        }
        
        suspend();
    }

    @Override
    public void process(Event e) throws Exception {

        replies++;
        
        if (replies == spawns) { 
            executor.send(identifier(), parent, null);
            finish();
        } else { 
            suspend();
        }
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
