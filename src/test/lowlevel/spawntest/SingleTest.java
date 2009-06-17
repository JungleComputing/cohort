package test.lowlevel.spawntest;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Context;
import ibis.cohort.Event;

public class SingleTest extends Activity {

    private static final long serialVersionUID = 5970093414747228592L;
    
    private final ActivityIdentifier parent;
    
    private final int spawns;
    private int replies;
    
    public SingleTest(ActivityIdentifier parent, int spawns) {
        super(Context.ANYWHERE);
        this.parent = parent;
        this.spawns= spawns;
    }

    @Override
    public void initialize() throws Exception {
        for (int i=0;i<spawns;i++) { 
            cohort.submit(new Dummy(identifier()));
        }
        
        suspend();
    }

    @Override
    public void process(Event e) throws Exception {

        replies++;
        
        if (replies == spawns) { 
            cohort.send(identifier(), parent, null);
            finish();
        } else { 
            suspend();
        }
    }

    @Override
    public void cleanup() throws Exception {
        // unused
    }

}
