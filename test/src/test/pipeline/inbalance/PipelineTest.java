package test.pipeline.inbalance;

import ibis.cohort.Cohort;
import ibis.cohort.CohortFactory;
import ibis.cohort.MultiEventCollector;

public class PipelineTest {

    public static void main(String [] args) { 
        
        // Simple test that creates, starts and stops a set of cohorts. When 
        // the lot is running, it deploys a series of jobs. 
        int jobs = Integer.parseInt(args[0]);
        int size = Integer.parseInt(args[1]);
      
        int rank = Integer.parseInt(args[2]);
        
        try {
            Cohort cohort = CohortFactory.createCohort();
            cohort.activate();
  
            if (rank == 0) { 

                long start = System.currentTimeMillis();

                MultiEventCollector me = new MultiEventCollector(jobs);

                cohort.submit(me);

                for (int i=0;i<jobs;i++) { 
                 
                    System.out.println("SUBMIT " + i);
         
                    Data data = new Data(i, 0, new byte[size]);
                    cohort.submit(new Stage1(me.identifier(), 100, data));
                }

                System.out.println("SUBMIT DONE");
          
                me.waitForEvents();

                long end = System.currentTimeMillis();

                System.out.println("Total processing time: " + (end-start) + " ms.");
            } 
            
            cohort.done();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
