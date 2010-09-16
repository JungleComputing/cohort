package test.fib;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Cohort;
import ibis.cohort.CohortFactory;
import ibis.cohort.Event;
import ibis.cohort.Executor;
import ibis.cohort.MessageEvent;
import ibis.cohort.SimpleExecutor;
import ibis.cohort.SingleEventCollector;
import ibis.cohort.context.UnitActivityContext;
import ibis.cohort.context.UnitWorkerContext;

public class Fibonacci extends Activity {

    private static final long serialVersionUID = 3379531054395374984L;

    private final ActivityIdentifier parent;

    private final int input;
    private int output;
    private int merged = 0;

    public Fibonacci(ActivityIdentifier parent, int input) {
        super(new UnitActivityContext("DEFAULT", input));
        this.parent = parent;
        this.input = input;
    }

    @Override
    public void initialize() throws Exception {

        if (input == 0 || input == 1) {
            output = input;
            finish();
        } else { 
            executor.submit(new Fibonacci(identifier(), input-1));
            executor.submit(new Fibonacci(identifier(), input-2));
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
            executor.send(identifier(), parent, output);
        } 
    }
    
    public String toString() { 
        return "Fib(" + identifier() + ") " + input + ", " + merged + " -> " + output;
    }

    public static void main(String [] args) throws Exception { 

        long start = System.currentTimeMillis();

        // Hmmm... this is not what we want. We want small jobs locally, and big jobs remote....
        Executor e1 = new SimpleExecutor(new UnitWorkerContext("DEFAULT", UnitWorkerContext.BIGGEST));
        Executor e2 = new SimpleExecutor(new UnitWorkerContext("DEFAULT", UnitWorkerContext.BIGGEST));
        
        Cohort cohort = CohortFactory.createColony(e1, e2);
        cohort.activate();
        
        int index = 0;
        
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
            cohort.done();
        }
    }
   
    @Override
    public void cancel() throws Exception {
        // TODO Auto-generated method stub
        
    }


}
