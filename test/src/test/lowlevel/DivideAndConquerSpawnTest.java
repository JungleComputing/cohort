package test.lowlevel;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Cohort;
import ibis.cohort.CohortFactory;
import ibis.cohort.Event;
import ibis.cohort.MessageEvent;
import ibis.cohort.SingleEventCollector;
import ibis.cohort.context.UnitActivityContext;

public class DivideAndConquerSpawnTest extends Activity {

    /*
     * This is the cohort equivalent of the SpawnOverhead test in Satin
     */
    
    
    private static final long serialVersionUID = 3379531054395374984L;
    
    private final ActivityIdentifier parent;

    private static final int SPAWNS_PER_SYNC = 10;
    private static final int COUNT = 1000000;    
    private static final int REPEAT = 10;
    
    private final boolean spawn; 
    
    private int merged = 0;
    
    private int repeat = 0;
    private int test = 0;
    
    private long start;
    
    public DivideAndConquerSpawnTest(ActivityIdentifier parent, boolean spawn) {
        super(UnitActivityContext.DEFAULT);
        this.parent = parent;
        this.spawn = spawn;
    }

    private void spawnAll() { 
        for (int i=0;i<SPAWNS_PER_SYNC;i++) {
            executor.submit(new DivideAndConquerSpawnTest(identifier(), false));                
        }
    }
    
    @Override
    public void initialize() throws Exception {

        if (spawn) {
            
            start = System.currentTimeMillis();
            
            spawnAll();            
            suspend();            
        } else {
            executor.send(identifier(), parent, 1);
            finish();
        }                        
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process(Event e) throws Exception {
        
        merged++;
      
        if (merged < SPAWNS_PER_SYNC) { 
            suspend();
            return;
        }
        
        // We have finished one set of spawns
        merged = 0;
        test++;
            
        if (test <= COUNT) {            
            spawnAll();            
            suspend();
            return;
        }
        
        // We have finished one iteration
        long end = System.currentTimeMillis();
            
        double timeSatin = (double) (end - start) / 1000.0;
        double cost =  ((double) (end - start) * 1000.0) / (SPAWNS_PER_SYNC * COUNT);
                
        System.out.println("spawn = " + timeSatin + " s, time/spawn = " + cost + " us/spawn" );
            
        test = 0;
        repeat++;
        
        if (repeat < REPEAT) {
            start = System.currentTimeMillis();
            spawnAll();
            suspend();
            return;    
        }       
            
        // We have finished completely
        finish();
    }

    @Override
    public void cleanup() throws Exception {
        // empty!
    }
    
    public static void main(String [] args) throws Exception { 

      //  Cohort cohort = new Sequential();

        Cohort cohort = CohortFactory.createCohort();
        
        int index = 0;
        
        SingleEventCollector a = new SingleEventCollector();

        cohort.submit(a);
        cohort.submit(new DivideAndConquerSpawnTest(a.identifier(), true));

        long result = ((MessageEvent<Long>)a.waitForEvent()).message;
     
        cohort.done();

    }

    @Override
    public void cancel() throws Exception {
        // TODO Auto-generated method stub
        
    }
}

