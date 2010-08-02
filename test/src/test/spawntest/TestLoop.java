package test.spawntest;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Context;
import ibis.cohort.Event;
import ibis.cohort.context.UnitContext;

public class TestLoop extends Activity {

    private static final long serialVersionUID = 5970093414747228592L;
    
    private final ActivityIdentifier parent;
    
    private final long count;
    private final int spawns;
    
    private int i;
    
    private long start;
    private long end;
    
    public TestLoop(ActivityIdentifier parent, long count, int spawns) {
        super(UnitContext.DEFAULT);
        this.parent = parent;
        this.count = count;
        this.spawns = spawns;
    }

    @Override
    public void initialize() throws Exception {
        
        start = System.currentTimeMillis();
        
        cohort.submit(new SingleTest(identifier(), spawns));
        suspend();
    }

    @Override
    public void process(Event e) throws Exception {

        i++;
        
        if (i == count) {             
            end = System.currentTimeMillis();            
            finish();
        } else {
            cohort.submit(new SingleTest(identifier(), spawns));
            suspend();
        }
    }

    @Override
    public void cleanup() throws Exception {
        
        double timeSatin = (double) (end - start) / 1000.0;
        double cost =  ((double) (end - start) * 1000.0) / (spawns * count);
                
        System.out.println("spawn = " + timeSatin + " s, time/spawn = " + cost + " us/spawn" );
        
        cohort.send(identifier(), parent, null);
    }

    @Override
    public void cancel() throws Exception {
        // not used        
    }
}
