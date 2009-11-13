package test.fib;

import ibis.cohort.Activity;
import ibis.cohort.Cohort;
import ibis.cohort.Context;
import ibis.cohort.Event;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.MessageEvent;
import ibis.cohort.SingleEventCollector;
import ibis.cohort.impl.distributed.DistributedCohort;
import ibis.cohort.impl.multithreaded.MTCohort;
import ibis.cohort.impl.sequential.Sequential;

public class Fibonacci extends Activity {

    private static final long serialVersionUID = 3379531054395374984L;

    private final ActivityIdentifier parent;

    private final int input;
    private int output;
    private int merged = 0;

    public Fibonacci(ActivityIdentifier parent, int input) {
        super(Context.ANY);
        this.parent = parent;
        this.input = input;
    }

    @Override
    public void initialize() throws Exception {

        if (input == 0 || input == 1) {
            output = input;
            finish();
        } else { 
            cohort.submit(new Fibonacci(identifier(), input-1));
            cohort.submit(new Fibonacci(identifier(), input-2));
            suspend();
        } 
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process(Event e) throws Exception {
        
        output += ((MessageEvent<Integer>) e).message;

        merged++;
      
      //  System.out.println("FIB " + input + " " + merged + " " + output);
        
        if (merged < 2) { 
            suspend();
        } else { 
            finish();
        }
    }

    @Override
    public void cleanup() throws Exception {
        if (parent != null) {
            cohort.send(identifier(), parent, output);
        } 
    }
    
    public String toString() { 
        return "Fib(" + identifier() + ") " + input + ", " + merged + " -> " + output;
    }

    public static void main(String [] args) throws Exception { 

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
        
        } else  if (args[index].equals("dist")) { 
            index++;
            cohort = new DistributedCohort(null);
       
            System.out.println("Using DISTRIBUTED Cohort implementation");
            
        } else { 
            System.out.println("Unknown Cohort implementation selected!");
            System.exit(1);
        }
        
        int input = Integer.parseInt(args[index++]);

        if (cohort.isMaster()) { 
       
            System.out.println("Starting as master!");
            
            SingleEventCollector a = new SingleEventCollector();

            cohort.submit(a);
            cohort.submit(new Fibonacci(a.identifier(), input));

            int result = ((MessageEvent<Integer>)a.waitForEvent()).message;

            cohort.done();

            long end = System.currentTimeMillis();

            System.out.println("FIB: Fib(" + input + ") = " + result + " (" 
                    + (end-start) + ")");
        } else { 
            
            System.out.println("Starting as slave!");
            
            while (true) { 
                Thread.sleep(10000);
            }
        }
    }
   
    @Override
    public void cancel() throws Exception {
        // TODO Auto-generated method stub
        
    }


}
