package ibis.constellation;

import ibis.constellation.context.UnitActivityContext;
import ibis.constellation.extra.ConstellationLogger;

import org.apache.log4j.Logger;

public class SingleEventCollector extends Activity {

    public static final Logger logger = ConstellationLogger.getLogger(SingleEventCollector.class);

    private static final long serialVersionUID = -538414301465754654L;

    private Event event;
    private final boolean verbose;

    public SingleEventCollector(ActivityContext c, boolean verbose) {
        super(c, true, true);
        this.verbose = verbose;
    }

    public SingleEventCollector(ActivityContext c) {
        this(c, false);
    }

    public SingleEventCollector() {
        this(UnitActivityContext.DEFAULT, false);
    }

    @Override
    public void initialize() throws Exception {
        suspend();
    }

    @Override
    public synchronized void process(Event e) throws Exception {

        if (verbose) {
            logger.info("SINGLE EVENT COLLECTOR ( " + identifier()
                    + ") GOT RESULT!");
        }

        event = e;
        notifyAll();
        finish();
    }

    public void cleanup() throws Exception {
        // empty
    }

    public void cancel() throws Exception {
        // empty
    }

    public String toString() {
        return "SingleEventCollector(" + identifier() + ")";
    }

    public synchronized Event waitForEvent() {
        while (event == null) {
            try {
                wait();
            } catch (Exception e) {
                // ignore
            }
        }

        return event;
    }
}
