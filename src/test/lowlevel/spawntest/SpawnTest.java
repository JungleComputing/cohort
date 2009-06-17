package test.lowlevel.spawntest;

import ibis.cohort.Cohort;
import ibis.cohort.SingleEventCollector;
import ibis.cohort.impl.multithreaded.MTCohort;
import ibis.cohort.impl.sequential.Sequential;

public class SpawnTest {
    
    // Cleaner version of the spawntest...
    
    private static final int SPAWNS_PER_SYNC = 10;
    private static final int COUNT = 1000000;    
    private static final int REPEAT = 10;
        
    public static void main(String [] args) { 

        //  Cohort cohort = new Sequential();

          Cohort cohort = null; 
          
          int index = 0;
          
          if (args[index].equals("seq")) { 
              cohort = new Sequential();
              index++;
              
              System.out.println("Using SEQUENTIAL Cohort implementation");
              
          } else  if (args[index].equals("mt")) { 
              index++;
              int threads = Integer.parseInt(args[index++]);
              cohort = new MTCohort(threads);
         
              System.out.println("Using MULTITHREADED(" + threads + ") Cohort implementation");
              
          } else { 
              System.out.println("Unknown Cohort implementation selected!");
              System.exit(1);
          }
    
          for (int i=0;i<REPEAT;i++) { 
              SingleEventCollector a = new SingleEventCollector();
              cohort.submit(a);
              cohort.submit(new TestLoop(a.identifier(), COUNT, SPAWNS_PER_SYNC));
              a.waitForEvent();
          }
       
          cohort.done();

      }
    
    
    
}
