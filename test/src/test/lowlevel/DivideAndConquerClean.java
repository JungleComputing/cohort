package test.lowlevel;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Cohort;
import ibis.cohort.CohortFactory;
import ibis.cohort.Context;
import ibis.cohort.Event;
import ibis.cohort.MessageEvent;
import ibis.cohort.SingleEventCollector;
import ibis.cohort.context.UnitContext;

public class DivideAndConquerClean extends Activity {

    /*
     * This is a simple divide and conquer example. The user can specify the branch factor 
     * and tree depth on the command line. All the application does is calculate the sum of 
     * the number of nodes in each subtree. 
     */
    
    private static final long serialVersionUID = 3379531054395374984L;

    private final ActivityIdentifier parent;

    private final int branch;
    private final int depth;
    
    private int merged = 0;    
    private long count = 1;
    
    public DivideAndConquerClean(ActivityIdentifier parent, int branch, int depth) {
        super(UnitContext.DEFAULT_ANYWHERE);
        this.parent = parent;
        this.branch = branch;
        this.depth = depth;
    }

    @Override
    public void initialize() throws Exception {

        if (depth == 0) {
            finish();
        } else {
            for (int i=0;i<branch;i++) { 
                cohort.submit(new DivideAndConquerClean(identifier(), branch, depth-1));
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

    public static void main(String [] args) throws Exception { 

        long start = System.currentTimeMillis();

        Cohort cohort = CohortFactory.createCohort();
        
        int index = 0;
        
        int branch = Integer.parseInt(args[index++]);
        int depth =  Integer.parseInt(args[index++]);
        
        long count = 0;
        
        for (int i=0;i<=depth;i++) { 
           count += Math.pow(branch, i);
        }
        
        System.out.println("Running D&C with branch factor " + branch + " and depth " 
                + depth + " (expected jobs: " + count + ")");
        
        SingleEventCollector a = new SingleEventCollector();

        cohort.submit(a);
        cohort.submit(new DivideAndConquerClean(a.identifier(), branch, depth));

        long result = ((MessageEvent<Long>)a.waitForEvent()).message;

        long end = System.currentTimeMillis();

        double nsPerJob = (1000.0*1000.0 * (end-start)) / count;
        
        String correct = (result == count) ? " (CORRECT)" : " (WRONG!)";
        
        System.out.println("D&C(" + branch + ", " + depth + ") = " + result + 
                correct + " total time = " + (end-start) + " job time = " + nsPerJob + " nsec/job");

        cohort.done();

    }

    @Override
    public void cancel() throws Exception {
        // TODO Auto-generated method stub
        
    }


}
