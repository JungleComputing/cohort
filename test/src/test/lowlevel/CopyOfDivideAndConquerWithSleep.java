package test.lowlevel;

import ibis.cohort.Activity;
import ibis.cohort.Cohort;
import ibis.cohort.CohortFactory;
import ibis.cohort.Context;
import ibis.cohort.Event;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.MessageEvent;
import ibis.cohort.SingleEventCollector;

public class CopyOfDivideAndConquerWithSleep extends Activity {

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
    private long count = 1;

    public CopyOfDivideAndConquerWithSleep(ActivityIdentifier parent, int branch, 
            int depth, int load) {
        super(Context.ANY);
        this.parent = parent;
        this.branch = branch;
        this.depth = depth;
        this.load = load;
    }

    @Override
    public void initialize() throws Exception {

        if (depth == 0) {            

            if (load > 0) { 
                try { 
                    Thread.sleep(load);
                } catch (Exception e) {
                    // ignore
                }
            }

            finish();
        } else {
            for (int i=0;i<branch;i++) { 
                cohort.submit(new CopyOfDivideAndConquerWithSleep(identifier(),
                        branch, depth-1, load));
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

    public String toString() { 
        return "DC(" + identifier() + ") " + branch + ", " + depth + ", " 
        + merged + " -> " + count;
    }

    public static void main(String [] args) { 
        
        try {
            long start = System.currentTimeMillis();

            Cohort cohort = CohortFactory.createCohort();
        
            if (cohort.isMaster()) { 

                int branch = Integer.parseInt(args[0]);
                int depth =  Integer.parseInt(args[1]);
                int load =  Integer.parseInt(args[2]);
                int workers = Integer.parseInt(args[3]);

                long count = 0;

                for (int i=0;i<=depth;i++) { 
                    count += Math.pow(branch, i);
                }

                double time = (load * Math.pow(branch, depth)) / (1000*workers); 

                System.out.println("Running D&C with branch factor " + branch + 
                        " and depth " + depth + " load " + load + 
                        " (expected jobs: " + count + ", expected time: " + 
                        time + " sec.)");

                SingleEventCollector a = new SingleEventCollector();

                cohort.submit(a);
                cohort.submit(new CopyOfDivideAndConquerWithSleep(a.identifier(), branch, 
                        depth, load));

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

    @Override
    public void cancel() throws Exception {
        // TODO Auto-generated method stub

    }


}
