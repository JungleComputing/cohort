package test.fib;

import ibis.cohort.Cohort;
import ibis.cohort.Job;
import ibis.cohort.Context;
import ibis.cohort.ParallelJob;
import ibis.cohort.impl.Sequential;

public class Fibonacci extends ParallelJob {

    private static final long serialVersionUID = 3379531054395374984L;

    public final int input;
    public int output;

    public Fibonacci( int input) {
        super(Context.ANYWHERE);
        this.input = input;
    }

    @Override
    public void merge(Job job, Object result) {
        this.output += (Integer) result;
    }

    @Override
    public void split() throws Exception {

        if (input == 0 || input == 1) {
            output = input;
        } else { 
            submit(new Fibonacci(input-1));
            submit(new Fibonacci(input-2));
        }
    }

    @Override
    public Object produceResult() {
        return output;
    }

    public String toString() {
        return identifier() + " : " + input;
    }
    
    public static void main(String [] args) { 

        long start = System.currentTimeMillis();

        Cohort cohort = new Sequential();

        int input = Integer.parseInt(args[0]);

        int result = (Integer) cohort.submit(new Fibonacci(input));

        cohort.done();

        long end = System.currentTimeMillis();

        System.out.println("FIB: Fib(" + input + ") = " + result + " (" 
                + (end-start) + ")");
    }
}
