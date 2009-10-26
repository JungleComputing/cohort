package test.lowlevel;

import ibis.cohort.Activity;
import ibis.cohort.Cohort;
import ibis.cohort.CohortFactory;
import ibis.cohort.Context;
import ibis.cohort.Event;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.MessageEvent;
import ibis.cohort.SingleEventCollector;

public class DivideAndConquerWithLoadAndContext extends Activity {

    /*
     * This is a simple divide and conquer example. The user can specify the branch factor 
     * and tree depth on the command line. All the application does is calculate the sum of 
     * the number of nodes in each subtree. 
     */

    private static final long serialVersionUID = 3379531054395374984L;

    private final ActivityIdentifier parent;

    private final int branch;
    private final int depth;
    private final int load;

    private int merged = 0;    
    private long took = 0;
    
    public DivideAndConquerWithLoadAndContext(ActivityIdentifier parent, 
            int branch, int depth, int load, Context c) {
        super(c);
       
      //  System.out.println("Creating job with Context " + c);
        
        this.parent = parent;
        this.branch = branch;
        this.depth = depth;
        this.load = load;
    }

    @Override
    public void initialize() throws Exception {

        if (depth == 0) {            

            if (load > 0) {
                
                long start = System.currentTimeMillis();
                long time = 0;
                
                while (time < load) { 
                    time = System.currentTimeMillis() - start;
                }
                
                took = time;
            }

            finish();
        } else {
            Context even = new Context("Even");
            Context odd = new Context("Odd");
            
            for (int i=0;i<branch;i++) { 
                Context tmp = (i % 2) == 0 ? even : odd;
                cohort.submit(new DivideAndConquerWithLoadAndContext(
                        identifier(), branch, depth-1, load, tmp));
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
        cohort.send(identifier(), parent, took);        
    }

    public String toString() { 
        return "DC(" + identifier() + ") " + branch + ", " + depth + ", " 
            + merged + " -> " + took;
    }

    public static void main(String [] args) { 
        
        try {        
            Cohort cohort = CohortFactory.createCohort();
        
            int branch = Integer.parseInt(args[0]);
            int depth =  Integer.parseInt(args[1]);
            int load =  Integer.parseInt(args[2]);
            int workers = Integer.parseInt(args[3]);
            
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
                System.out.println("Setting context to Even");
                
                cohort.setContext(new Context("Even"));
            } else { 
                // odd
                System.out.println("Setting context to Odd");
                
                cohort.setContext(new Context("Odd"));
            }
            
            if (cohort.isMaster()) { 

                long count = 0;

                for (int i=0;i<=depth;i++) { 
                    count += Math.pow(branch, i);
                }

                double time = (load * Math.pow(branch, depth)) / (1000*workers); 

                System.out.println("Running D&C with branch factor " + branch + 
                        " and depth " + depth + " load " + load + 
                        " (expected jobs: " + count + ", expected time: " + 
                        time + " sec.)");

                long start = System.currentTimeMillis();
                
                SingleEventCollector a = new SingleEventCollector();

                cohort.submit(a);
                cohort.submit(new DivideAndConquerWithLoadAndContext(
                        a.identifier(), branch, depth, load, 
                        new Context("Even")));

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

    @Override
    public void cancel() throws Exception {
        // TODO Auto-generated method stub

    }


}
