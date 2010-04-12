package test.lowlevel;

import ibis.cohort.Activity;
import ibis.cohort.Cohort;
import ibis.cohort.CohortFactory;
import ibis.cohort.CohortIdentifier;
import ibis.cohort.Context;
import ibis.cohort.Event;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.MessageEvent;
import ibis.cohort.SingleEventCollector;
import ibis.cohort.context.UnitContext;

public class DivideAndConquerWithContextAndPenalty extends Activity {

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
    private long count = 1;

    public DivideAndConquerWithContextAndPenalty(ActivityIdentifier parent, 
            int branch, int depth, int sleep, int penalty, Context context) {
        super(context);
        this.parent = parent;
        this.branch = branch;
        this.depth = depth;
        this.sleep = sleep;
        this.penalty = penalty;
    }

    @Override
    public void initialize() throws Exception {

        if (depth == 0) {            

            if (sleep > 0) { 
                try { 
                    Thread.sleep(sleep);
                } catch (Exception e) {
                    // ignore
                }
            }

            finish();
        } else {
            
            Context even = new UnitContext("Even");
            Context odd = new UnitContext("Odd");
                
            for (int i=0;i<branch;i++) { 
                Context tmp = (i % 2) == 0 ? even : odd;
                cohort.submit(new DivideAndConquerWithContextAndPenalty(
                        identifier(), branch, depth-1, sleep, penalty, tmp));
            }
            suspend();
        } 
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process(Event e) throws Exception {

        count += ((MessageEvent<Long>) e).message;

        merged++;

        if (merged < branch) { 
            suspend();
        } else { 
            finish();
        }
    }

    @Override
    public void cleanup() throws Exception {
        cohort.send(identifier(), parent, count);        
    }

    @Override
    public void cancel() throws Exception {
        // TODO Auto-generated method stub

    }
    
    public String toString() { 
        return "DC(" + identifier() + ") " + branch + ", " + depth + ", " 
        + merged + " -> " + count;
    }

    public static void main(String [] args) { 
        
        try {
            long start = System.currentTimeMillis();

            Cohort cohort = CohortFactory.createCohort();
        
            Context even = new UnitContext("Even");
            Context odd = new UnitContext("Odd");
           
            int branch = Integer.parseInt(args[0]);
            int depth =  Integer.parseInt(args[1]);
            int sleep =  Integer.parseInt(args[2]);
            int penalty =  Integer.parseInt(args[3]);
            int workers = Integer.parseInt(args[4]);
            int rank = Integer.parseInt(args[5]);
            
            CohortIdentifier [] leafs = cohort.getLeafIDs();

            System.out.println("Setting context...");
            
            for (CohortIdentifier id : leafs) { 
                if (rank % 2 == 0) {
                    System.out.println("Setting context to " + even);
                    cohort.setContext(id, even);
                } else { 
                    System.out.println("Setting context to " + odd);
                    cohort.setContext(id, odd);
                }
            }

            cohort.activate();
            
            if (cohort.isMaster()) { 
           
                long count = 0;

                for (int i=0;i<=depth;i++) { 
                    count += Math.pow(branch, i);
                }

                double min = (sleep * Math.pow(branch, depth)) / (1000*workers); 
                double max = ((sleep * penalty) * Math.pow(branch, depth)) / (1000*workers); 
                
                System.out.println("Running D&C with branch factor " + branch + 
                        " and depth " + depth + " sleep " + sleep + 
                        " (expected jobs: " + count + ", expected time: " + 
                        min  + " ~ " + max + " sec.)");

                SingleEventCollector a = new SingleEventCollector();

                cohort.submit(a);
                cohort.submit(new DivideAndConquerWithContextAndPenalty(
                        a.identifier(), branch, depth, sleep, penalty, even));

                long result = ((MessageEvent<Long>)a.waitForEvent()).message;

                long end = System.currentTimeMillis();

                double nsPerJob = (1000.0*1000.0 * (end-start)) / count;

                String correct = (result == count) ? " (CORRECT)" : " (WRONG!)";

                System.out.println("D&C(" + branch + ", " + depth + ") = " + result + 
                        correct + " total time = " + (end-start) + " job time = " + 
                        nsPerJob + " nsec/job");

            }
            
            cohort.done();
        
        } catch (Exception e) {
            System.err.println("Oops: " + e);
            e.printStackTrace(System.err);
            System.exit(1);
        }

    }

    

}
