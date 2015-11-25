package test.fib;

import ibis.constellation.Activity;
import ibis.constellation.ActivityIdentifier;
import ibis.constellation.Constellation;
import ibis.constellation.ConstellationFactory;
import ibis.constellation.Event;
import ibis.constellation.Executor;
import ibis.constellation.SimpleExecutor;
import ibis.constellation.SingleEventCollector;
import ibis.constellation.StealStrategy;
import ibis.constellation.context.UnitActivityContext;
import ibis.constellation.context.UnitWorkerContext;

public class Fibonacci extends Activity {

    private static final long serialVersionUID = 3379531054395374984L;

    private final ActivityIdentifier parent;

    private final boolean top;
    private final int input;
    private int output;
    private int merged = 0;

    public Fibonacci(ActivityIdentifier parent, int input, boolean top) {
        super(new UnitActivityContext("DEFAULT", input), input > 1);
        this.parent = parent;
        this.input = input;
        this.top = top;

        if (top) {
            System.out.println("fib " + input + " created.");
        }
    }

    public Fibonacci(ActivityIdentifier parent, int input) {
        this(parent, input, false);
    }

    @Override
    public void initialize() throws Exception {

        if (top) {
            System.out.println("fib " + input + " running.");
        }

        if (input == 0 || input == 1) {
            output = input;
            finish();
        } else {
            executor.submit(new Fibonacci(identifier(), input - 1));
            executor.submit(new Fibonacci(identifier(), input - 2));
            suspend();
        }
    }

    @Override
    public void process(Event e) throws Exception {

        if (top) {
            System.out.println("fib " + input + " got event.");
        }

        output += (Integer) e.data;
        merged++;

        if (merged < 2) {
            suspend();
        } else {
            finish();
        }
    }

    @Override
    public void cleanup() throws Exception {

        if (top) {
            System.out.println("fib " + input + " done.");
        }

        if (parent != null) {
            executor.send(new Event(identifier(), parent, output));
        }
    }

    public String toString() {
        return "Fib(" + identifier() + ") " + input + ", " + merged + " -> "
                + output;
    }

    public static void main(String[] args) throws Exception {

        long start = System.currentTimeMillis();

        int index = 0;

        int executors = Integer.parseInt(args[index++]);

        Executor[] e = new Executor[executors];

        for (int i = 0; i < executors; i++) {
            e[i] = new SimpleExecutor(new UnitWorkerContext("DEFAULT"),
                    StealStrategy.SMALLEST, StealStrategy.BIGGEST);
        }

        Constellation c = ConstellationFactory.createConstellation(e);
        c.activate();

        int input = Integer.parseInt(args[index++]);

        if (c.isMaster()) {

            System.out.println("Starting as master!");

            SingleEventCollector a = new SingleEventCollector();

            ActivityIdentifier aid = c.submit(a);
            c.submit(new Fibonacci(aid, input, true));

            int result = (Integer) a.waitForEvent().data;

            c.done();

            long end = System.currentTimeMillis();

            System.out.println("FIB: Fib(" + input + ") = " + result + " ("
                    + (end - start) + ")");
        } else {
            System.out.println("Starting as slave!");
            c.done();
        }
    }

    @Override
    public void cancel() throws Exception {
        // not used
    }
}
