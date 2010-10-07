package test.lowlevel;

import ibis.constellation.Activity;
import ibis.constellation.ActivityContext;
import ibis.constellation.ActivityIdentifier;
import ibis.constellation.Cohort;
import ibis.constellation.CohortFactory;
import ibis.constellation.CohortIdentifier;
import ibis.constellation.Event;
import ibis.constellation.MessageEvent;
import ibis.constellation.SingleEventCollector;
import ibis.constellation.WorkerContext;
import ibis.constellation.context.UnitActivityContext;
import ibis.constellation.context.UnitWorkerContext;

public class DivideAndConquerWithContextPenalty extends Activity {

    /*
     * This is a simple divide and conquer example. The user can specify the branch factor 
     * and tree depth on the command line. All the application does is calculate the sum of 
     * the number of nodes in each subtree. 
     */
    
    private static final long serialVersionUID = 3379531054395374984L;

    private final ActivityIdentifier parent;

    private final int branch;
    private final int depth;
    private final int sleep;
    private final int penalty;

    private int merged = 0;    
    private long took = 0;
    
    public DivideAndConquerWithContextPenalty(ActivityIdentifier parent, 
            int branch, int depth, int sleep, int penalty, ActivityContext c) {
      
        super(c);
       
      //  System.out.println("Creating job with Context " + c);
        
        this.parent = parent;
        this.branch = branch;
        this.depth = depth;
        this.sleep = sleep;
        this.penalty = penalty;
    }

    @Override
    public void initialize() throws Exception {
        
        if (depth == 0) {            

            long time = sleep;
            
            WorkerContext machineContext = executor.getContext();
            ActivityContext activitycontext = getContext();
            
            if (machineContext == null || machineContext.equals(UnitWorkerContext.DEFAULT)) { 
            
                // Check if context stored in LocalData is same as activity 
                // context. If not, add penalty to time.
          
                machineContext = (WorkerContext) LocalData.getLocalData().get("context");
                
                if (!activitycontext.equals(machineContext)) { 
                    time = time * penalty;
                }
            }
       
            try { 
                Thread.sleep(time);
            } catch (Exception e) {
                // ignored
            }
            
            finish();
        } else {
            ActivityContext even = new UnitActivityContext("Even");
            ActivityContext odd = new UnitActivityContext("Odd");
            
            for (int i=0;i<branch;i++) { 
                ActivityContext tmp = (i % 2) == 0 ? even : odd;
                executor.submit(new DivideAndConquerWithContextPenalty(
                        identifier(), branch, depth-1, sleep, penalty, tmp));
            }
            suspend();
        } 
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process(Event e) throws Exception {

        took += ((MessageEvent<Long>) e).message;

        merged++;

        if (merged < branch) { 
            suspend();
        } else { 
            finish();
        }
    }

    @Override
    public void cleanup() throws Exception {
        executor.send(identifier(), parent, took);        
   
        System.out.println("Finished job");
    }

    public String toString() { 
        return "DC(" + identifier() + ") " + getContext() + ", " +  branch 
            + ", " + depth + ", " + merged + " -> " + took;
    }

    @Override
    public void cancel() throws Exception {
        // TODO Auto-generated method stub

    }
    
    public static void main(String [] args) { 
        
        try {        
            Cohort cohort = CohortFactory.createCohort();
        
            int branch = Integer.parseInt(args[0]);
            int depth =  Integer.parseInt(args[1]);
            int sleep =  Integer.parseInt(args[2]);
            int penalty =  Integer.parseInt(args[3]);
            int workers = Integer.parseInt(args[4]);
            
            boolean forceContext = true;
            
            if (args.length > 5) { 
                forceContext = args[5].equals("force");
           
                System.out.println("Force set to " + forceContext + " " + args[5]);
            
            }
            
            // NOTE: this is just a simply way to divide all machines into 
            // two groups, "even" and "odd". This allows us to test the context 
            // aware stealing. 
            
            String tmp = System.getProperty("ibis.cohort.rank");
            
            if (tmp == null) { 
                System.err.println("Cannot determine machine rank!");
                System.exit(1);
            }
            
            
            int rank = Integer.parseInt(tmp);

            if ((rank % 2) == 0) { 
                // even
           
                if (forceContext)  {
                    System.out.println("Forcing context to Even");
                    
                    CohortIdentifier [] leafs = cohort.getLeafIDs();
                    
                    UnitWorkerContext c = new UnitWorkerContext("Even");
                    
                    for (CohortIdentifier id : leafs) { 
                        cohort.setContext(id, c);
                    }
                } else { 
                    System.out.println("LocalData context set to Even");
                    LocalData.getLocalData().put("context", new UnitWorkerContext("Even"));
                }
            } else { 
                // odd
                System.out.println("Setting context to Odd");

                if (forceContext)  {
                    System.out.println("Forcing context to Odd");
                    
                    CohortIdentifier [] leafs = cohort.getLeafIDs();
                    
                    UnitWorkerContext c = new UnitWorkerContext("Odd");
                    
                    for (CohortIdentifier id : leafs) { 
                        cohort.setContext(id, c);
                    }
                
                } else { 
                    System.out.println("LocalData context set to Odd");
                    LocalData.getLocalData().put("context", new UnitWorkerContext("Odd"));
                }   
            } 
            
            cohort.activate();
            
            if (cohort.isMaster()) { 

                long count = 0;

                for (int i=0;i<=depth;i++) { 
                    count += Math.pow(branch, i);
                }
                                
                double min = (sleep * Math.pow(branch, depth)) / (1000*workers); 
                double max = ((sleep*penalty) * Math.pow(branch, depth)) / (1000*workers); 
                
                System.out.println("Running D&C with branch factor " + branch 
                        + " and depth " + depth + " sleep " + sleep 
                        + " penalty " + penalty + " (expected jobs: " + count 
                        + ", expected time: " + 
                        min + " to " + max + " sec.)");

                long start = System.currentTimeMillis();
                
                SingleEventCollector a = new SingleEventCollector();

                cohort.submit(a);
                cohort.submit(new DivideAndConquerWithContextPenalty(
                        a.identifier(), branch, depth, sleep, penalty,
                        new UnitActivityContext("Even")));

                long result = ((MessageEvent<Long>)a.waitForEvent()).message;

                long end = System.currentTimeMillis();

                double msPerJob = ((double)(end-start)) / count;

                System.out.println("D&C(" + branch + ", " + depth + ") " 
                        + " wall clock time = " + (end-start) 
                        + " processing time = " + result  
                        + " avg job time = " + msPerJob + " msec/job");

            }
            
            cohort.done();
        
        } catch (Exception e) {
            System.err.println("Oops: " + e);
            e.printStackTrace(System.err);
            System.exit(1);
        }

    }


}
