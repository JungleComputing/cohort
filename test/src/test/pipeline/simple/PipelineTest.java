package test.pipeline.simple;

import java.util.Properties;

import ibis.constellation.Constellation;
import ibis.constellation.ConstellationFactory;
import ibis.constellation.MultiEventCollector;
import ibis.constellation.SimpleExecutor;

public class PipelineTest {

    public static void main(String [] args) { 
        
        // Simple test that creates, starts and stops a set of constellations. When 
        // the lot is running, it deploys a series of jobs. 
        
        int nodes = Integer.parseInt(args[0]);
        int rank = Integer.parseInt(args[1]);
        int threads = Integer.parseInt(args[2]);
        
        int jobs = Integer.parseInt(args[3]);
        long sleep = Long.parseLong(args[4]);
        int data = Integer.parseInt(args[5]);
        
        String config = "dist+mt+["; 
        
        for (int i=0;i<threads;i++) { 
            config += "st:c" + (rank*threads+i);
            if (i != threads-1) { 
                config += ",";
            }
        }
        
        Properties p = System.getProperties();
        p.put("ibis.cohort.impl", config);
        
        try {
            Constellation constellation = ConstellationFactory.createConstellation(new SimpleExecutor());
            constellation.activate();
  
            if (rank == 0) { 

                long start = System.currentTimeMillis();

                MultiEventCollector me = new MultiEventCollector(jobs);

                constellation.submit(me);

                for (int i=0;i<jobs;i++) { 
                 
                    System.out.println("SUBMIT " + i);
                    
                    constellation.submit(new Pipeline(me.identifier(), i, 0, 
                            nodes*threads-1, sleep, new byte[data]));
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
