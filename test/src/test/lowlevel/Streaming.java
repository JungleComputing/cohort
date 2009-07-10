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

public class Streaming extends Activity {
    
    /*
     * This is a simple streaming example. A sequence of activities is created (length 
     * specified on commandline). The first activity repeatedly sends and object to the 
     * second activity, which forwards it to the third, etc. Once all object have been    
     * received by the last activity, it sends a reply to the application.
     */
    
    private static final long serialVersionUID = 3379531054395374984L;

    private final ActivityIdentifier root;
    private ActivityIdentifier next;
    
    private final int length;
    private final int index;
    private final int totaldata;
    private int dataSeen;
    
    public Streaming(ActivityIdentifier root, int length, int index, int totaldata) {
        super(Context.ANY);
        this.root = root;
        this.length = length;
        this.index = index;
        this.totaldata = totaldata;
    }

    @Override
    public void initialize() throws Exception {

        if (index < length) {
            // Submit the next job in the sequence
            next = cohort.submit(new Streaming(root, length, index+1, totaldata));
        } 
  
        suspend();
    }

    @Override
    public void process(Event e) throws Exception {

        if (next != null) { 
            cohort.send(identifier(), next, ((MessageEvent) e).message);
        }
        
        dataSeen++;
        
        if (dataSeen == totaldata) { 
            finish();
        } else { 
            suspend();
        }
    }

    @Override
    public void cleanup() throws Exception {

        if (next == null) { 
            // only the last replies!
            cohort.send(identifier(), root, dataSeen);
        }
    }
    
    public String toString() { 
        return "Streaming(" + identifier() + ") " + length;
    }

    public static void main(String [] args) { 

        long start = System.currentTimeMillis();

        Cohort cohort = null; 
        
        int index = 0;
        
        if (args[index].equals("seq")) { 
            cohort = new Sequential();
            index++;
            
            System.out.println("Using SEQUENTIAL Cohort implementation");
            
        } else  if (args[index].equals("mt")) { 
            index++;
            int threads = Integer.parseInt(args[index++]);
            cohort = new MTCohort(threads);
       
            System.out.println("Using MULTITHREADED(" + threads + ") Cohort implementation");
            
        } else { 
            System.out.println("Unknown Cohort implementation selected!");
            System.exit(1);
        }
        
        int length = Integer.parseInt(args[index++]);
        int data = Integer.parseInt(args[index++]);
        
        System.out.println("Running Streaming with series length " + length 
                + " and " + data + " messages");
        
        SingleEventCollector a = new SingleEventCollector();
        cohort.submit(a);
        
        Streaming s = new Streaming(a.identifier(), length, 0, data);
        cohort.submit(s);

        for (int i=0;i<data;i++) { 
            cohort.send(a.identifier(), s.identifier(), i);
        }
        
        long result = ((MessageEvent<Integer>)a.waitForEvent()).message;

        long end = System.currentTimeMillis();

        double nsPerJob = (1000.0*1000.0 * (end-start)) / (data*length);
        
        String correct = (result == data) ? " (CORRECT)" : " (WRONG!)";
        
        System.out.println("Series(" + length + ", " + data + ") = " + result + 
                correct + " total time = " + (end-start) + 
                " job time = " + nsPerJob + " nsec/job");

        cohort.done();

    }

    @Override
    public void cancel() throws Exception {
        // TODO Auto-generated method stub
        
    }


}
