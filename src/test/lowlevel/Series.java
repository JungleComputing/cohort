package test.lowlevel;

import ibis.cohort.Activity;
import ibis.cohort.Cohort;
import ibis.cohort.Context;
import ibis.cohort.Event;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.MessageEvent;
import ibis.cohort.SingleEventCollector;
import ibis.cohort.impl.multithreaded.MTCohort;
import ibis.cohort.impl.sequential.Sequential;

public class Series extends Activity {
    
    /*
     * This is a simple series example. The user can specify the length of the series 
     * on the command line. All the application does is create a sequence of nodes until    
     * the specified length has been reached. The last node returns the result (the nodecount).  
     */
    
    private static final long serialVersionUID = 3379531054395374984L;

    private final ActivityIdentifier root;

    private final int length;
    private final int count;
    
    public Series(ActivityIdentifier root, int length, int count) {
        super(Context.ANYWHERE);
        this.root = root;
        this.length = length;
        this.count = count;
    }

    @Override
    public void initialize() throws Exception {

        if (count < length) {
            // Submit the next job in the series
            cohort.submit(new Series(root, length, count+1));
        } 
        
        finish();
    }

    @Override
    public void process(Event e) throws Exception {
        // Not used!
    }

    @Override
    public void cleanup() throws Exception {
        
        if (count == length) { 
            // Only the last job send a reply!
            cohort.send(identifier(), root, count);
        }
    }
    
    public String toString() { 
        return "Series(" + identifier() + ") " + length;
    }

    public static void main(String [] args) { 

        long start = System.currentTimeMillis();

        Cohort cohort = null; 
        
        if (args[0].equals("seq")) { 
            cohort = new Sequential();
        } else  if (args[0].equals("mt")) { 
            cohort = new MTCohort(1);
        } else { 
            System.out.println("Unknown Cohort implementation selected!");
            System.exit(1);
        }
        
        int length = Integer.parseInt(args[1]);
        
        System.out.println("Running Series with length " + length);
        
        SingleEventCollector a = new SingleEventCollector();

        cohort.submit(a);
        cohort.submit(new Series(a.identifier(), length, 0));

        long result = ((MessageEvent<Integer>)a.waitForEvent()).message;

        long end = System.currentTimeMillis();

        double nsPerJob = (1000.0*1000.0 * (end-start)) / length;
        
        String correct = (result == length) ? " (CORRECT)" : " (WRONG!)";
        
        System.out.println("Series(" + length + ") = " + result + 
                correct + " total time = " + (end-start) + 
                " job time = " + nsPerJob + " nsec/job");

        cohort.done();

    }


}
