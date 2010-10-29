package test.pipeline.inbalance;

import ibis.constellation.Constellation;
import ibis.constellation.ConstellationFactory;
import ibis.constellation.MultiEventCollector;
import ibis.constellation.SimpleExecutor;

public class PipelineTest {

    public static void main(String [] args) { 
        
        // Simple test that creates, starts and stops a set of constellations. When 
        // the lot is running, it deploys a series of jobs. 
        int jobs = Integer.parseInt(args[0]);
        int size = Integer.parseInt(args[1]);
      
        int rank = Integer.parseInt(args[2]);
        
        try {
            Constellation constellation = ConstellationFactory.createConstellation(new SimpleExecutor());
            constellation.activate();
  
            if (rank == 0) { 

                long start = System.currentTimeMillis();

                MultiEventCollector me = new MultiEventCollector(jobs);

                constellation.submit(me);

                for (int i=0;i<jobs;i++) { 
                 
                    System.out.println("SUBMIT " + i);
         
                    Data data = new Data(i, 0, new byte[size]);
                    constellation.submit(new Stage1(me.identifier(), 100, data));
                }

                System.out.println("SUBMIT DONE");
          
                me.waitForEvents();

                long end = System.currentTimeMillis();

                System.out.println("Total processing time: " + (end-start) + " ms.");
            } 
            
            constellation.done();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
