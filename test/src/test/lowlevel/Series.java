package test.lowlevel;

import ibis.constellation.Activity;
import ibis.constellation.ActivityIdentifier;
import ibis.constellation.Constellation;
import ibis.constellation.ConstellationFactory;
import ibis.constellation.Event;
import ibis.constellation.SimpleExecutor;
import ibis.constellation.SingleEventCollector;
import ibis.constellation.StealStrategy;
import ibis.constellation.context.UnitActivityContext;
import ibis.constellation.context.UnitWorkerContext;

public class Series extends Activity {

    /*
     * This is a simple series example. The user can specify the length of the
     * series on the command line. All the application does is create a sequence
     * of nodes until the specified length has been reached. The last node
     * returns the result (the nodecount).
     */

    private static final long serialVersionUID = 3379531054395374984L;

    private final ActivityIdentifier root;

    private final int length;
    private final int count;

    public Series(ActivityIdentifier root, int length, int count) {
        super(new UnitActivityContext("S", count), false);
        this.root = root;
        this.length = length;
        this.count = count;
    }

    @Override
    public void initialize() throws Exception {

        if (count < length) {
            // Submit the next job in the series
            executor.submit(new Series(root, length, count + 1));
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
            executor.send(new Event(identifier(), root, count));
        }
    }

    public String toString() {
        return "Series(" + identifier() + ") " + length;
    }

    public static void main(String[] args) throws Exception {

        long start = System.currentTimeMillis();

        int index = 0;

        int length = Integer.parseInt(args[index++]);

        System.out.println("Running Series with length " + length);

        Constellation c = ConstellationFactory.createConstellation(
                new SimpleExecutor(new UnitWorkerContext("S"),
                        StealStrategy.ANY));
        c.activate();

        if (c.isMaster()) {
            SingleEventCollector a = new SingleEventCollector(
                    new UnitActivityContext("S"));
            c.submit(a);
            c.submit(new Series(a.identifier(), length, 0));

            long result = (Long) a.waitForEvent().data;

            long end = System.currentTimeMillis();

            double nsPerJob = (1000.0 * 1000.0 * (end - start)) / length;

            String correct = (result == length) ? " (CORRECT)" : " (WRONG!)";

            System.out.println("Series(" + length + ") = " + result + correct
                    + " total time = " + (end - start) + " job time = "
                    + nsPerJob + " nsec/job");
        }
        c.done();
    }

    @Override
    public void cancel() throws Exception {
        // not used

    }

}
