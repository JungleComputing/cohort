package sor.rigid;

import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Cohort;
import ibis.cohort.CohortFactory;
import ibis.cohort.MultiEventCollector;
import ibis.cohort.context.UnitContext;

import java.util.Properties;

public class RigidSOR {

    /*
     * This is a rigid implementation of Successive OverRelaxation (SOR). It is 
     * essentially an SPMD application where each participating machine creates 
     * a single local activity for each of its cores. The application assumes 
     * that the number of machines, and the number of cores per machine is known 
     * in advance, and that the machines are homegeneous.
     */

    private static int setContexts(Cohort cohort, int start) throws Exception { 
        
        Cohort [] sub = cohort.getSubCohorts();
        
        if (sub == null) { 
            // We've arrived at the leaf Cohort.
         System.out.println("Setting context C-" + start);
            
            cohort.setContext(new UnitContext("C-" + start));
            start++;
        } else { 
            // More levels to go!
            for (Cohort tmp : cohort.getSubCohorts()) { 
                start = setContexts(tmp, start);
            }
        }     
        
        return start;
    }
    
    public static void main(String [] args) throws Exception { 
        
        // Parse command line parameters
        int machines = Integer.parseInt(args[0]);
        int coresPerMachine =  Integer.parseInt(args[1]);
        int rank = Integer.parseInt(args[2]);
        
        int N = Integer.parseInt(args[3]); // Problem size
        int itt = Integer.parseInt(args[4]);
            
        System.out.println("Running " + N + "x" + N + " SOR on " 
                + (machines * coresPerMachine) + " cores (" + machines 
                + "*" + coresPerMachine +")");
        
        // Create the required cohorts and set their contexts.
        Properties p = new Properties();
        p.setProperty("ibis.cohort.workers", "" + coresPerMachine);
        p.setProperty("ibis.cohort.impl", "dist");
        
        Cohort cohort = CohortFactory.createCohort(p);
        
        setContexts(cohort, rank*coresPerMachine);
        
        cohort.activate();
        
        if (cohort.isMaster()) { 

            long start = System.currentTimeMillis();
            
            int workers = machines*coresPerMachine;          
            
            MultiEventCollector a = new MultiEventCollector(workers);
            cohort.submit(a);
            
            // Generate all activities and submit them
            ActivityIdentifier [] activities = new ActivityIdentifier[workers];
            
            for (int i=0;i<workers;i++) { 
                activities[i] = cohort.submit(
                        new SOR(a.identifier(), workers, i, N, itt));
            }
    
            // Inform all activities of their neighbours' whereabouts 
            if (workers > 1) { 
                cohort.send(null, activities[0], new Neighbours(null, activities[1]));
                cohort.send(null, activities[workers-1], new Neighbours(activities[workers-2], null));
            
                for (int i=1;i<workers-1;i++) { 
                    cohort.send(null, activities[i], new Neighbours(activities[i-1], activities[i+1]));
                }
            }
            
            // Wait for the activities to finish
            a.waitForEvents();
         
            cohort.done();
            
            long end = System.currentTimeMillis();
            
            System.out.println("Done in " + (end-start) + " ms.");
        }
        
        cohort.done();
    }
}
