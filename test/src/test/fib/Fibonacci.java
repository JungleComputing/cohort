package test.fib;

import ibis.cohort.Activity;
import ibis.cohort.Cohort;
import ibis.cohort.Context;
import ibis.cohort.Event;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.MessageEvent;
import ibis.cohort.SingleEventCollector;
import ibis.cohort.impl.sequential.Sequential;

public class Fibonacci extends Activity {

    private static final long serialVersionUID = 3379531054395374984L;

    private final ActivityIdentifier parent;

    private final int input;
    private int output;
    private int merged = 0;

    public Fibonacci(ActivityIdentifier parent, int input) {
        super(Context.ANYWHERE);
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

    public static void main(String [] args) { 

        long start = System.currentTimeMillis();

        Cohort cohort = new Sequential();

        int input = Integer.parseInt(args[0]);

        SingleEventCollector a = new SingleEventCollector();

        cohort.submit(a);
        cohort.submit(new Fibonacci(a.identifier(), input));

        int result = ((MessageEvent<Integer>)a.waitForEvent()).message;

        cohort.done();

        long end = System.currentTimeMillis();

        System.out.println("FIB: Fib(" + input + ") = " + result + " (" 
                + (end-start) + ")");
    }


}
