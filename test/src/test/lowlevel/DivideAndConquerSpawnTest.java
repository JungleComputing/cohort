package test.lowlevel;

import ibis.constellation.Activity;
import ibis.constellation.ActivityIdentifier;
import ibis.constellation.Constellation;
import ibis.constellation.ConstellationFactory;
import ibis.constellation.Event;
import ibis.constellation.MessageEvent;
import ibis.constellation.SimpleExecutor;
import ibis.constellation.SingleEventCollector;
import ibis.constellation.StealStrategy;
import ibis.constellation.context.UnitActivityContext;
import ibis.constellation.context.UnitWorkerContext;

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
        super(UnitActivityContext.DEFAULT, true);
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
            executor.send(new MessageEvent(identifier(), parent, 1));
            finish();
        }                        
    }

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
        executor.send(new MessageEvent(identifier(), parent, 1L));
        finish();
    }

    @Override
    public void cleanup() throws Exception {
        // empty!
    }
    
    public static void main(String [] args) throws Exception { 

      //  Cohort cohort = new Sequential();

        Constellation constellation = ConstellationFactory.createConstellation(new SimpleExecutor(UnitWorkerContext.DEFAULT, StealStrategy.SMALLEST, StealStrategy.BIGGEST));
        constellation.activate();
        
        int index = 0;
        
        SingleEventCollector a = new SingleEventCollector();

        constellation.submit(a);
        constellation.submit(new DivideAndConquerSpawnTest(a.identifier(), true));

        long result = (Long)((MessageEvent)a.waitForEvent()).message;
     
        constellation.done();

    }

    @Override
    public void cancel() throws Exception {
        // TODO Auto-generated method stub
        
    }
}

