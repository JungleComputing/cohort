package test.spawntest;

import ibis.cohort.Cohort;
import ibis.cohort.CohortFactory;
import ibis.cohort.SingleEventCollector;

public class SpawnTest {
    
    // Cleaner version of the spawntest...
    
    private static final int SPAWNS_PER_SYNC = 10;
    private static final int COUNT = 1000000;    
    private static final int REPEAT = 10;
        
    public static void main(String [] args) { 
          
          try { 
              Cohort cohort = CohortFactory.createCohort(); 

              for (int i=0;i<REPEAT;i++) { 
                  SingleEventCollector a = new SingleEventCollector();
                  cohort.submit(a);
                  cohort.submit(new TestLoop(a.identifier(), COUNT, SPAWNS_PER_SYNC));
                  a.waitForEvent();
              }
              
              cohort.done();
          
          } catch (Exception e) {
              System.err.println("Oops: " + e);
              e.printStackTrace(System.err);
              System.exit(1);
          }
      }
    
    
    
}
