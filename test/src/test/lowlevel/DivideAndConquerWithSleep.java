package test.lowlevel;

import ibis.constellation.Activity;
import ibis.constellation.ActivityIdentifier;
import ibis.constellation.Constellation;
import ibis.constellation.ConstellationFactory;
import ibis.constellation.Event;
import ibis.constellation.Executor;
import ibis.constellation.MessageEvent;
import ibis.constellation.SimpleExecutor;
import ibis.constellation.SingleEventCollector;
import ibis.constellation.StealStrategy;
import ibis.constellation.context.UnitActivityContext;
import ibis.constellation.context.UnitWorkerContext;

public class DivideAndConquerWithSleep extends Activity {

    /*
     * This is a simple divide and conquer example. The user can specify the branch factor 
     * and tree depth on the command line. All the application does is calculate the sum of 
     * the number of nodes in each subtree. 
     */

    private static final long serialVersionUID = 3379531054395374984L;

    private final ActivityIdentifier parent;

    private final int branch;
    private final int depth;
    private final int load;

    private int merged = 0;    
    private long count = 1;

    public DivideAndConquerWithSleep(ActivityIdentifier parent, int branch, 
            int depth, int load) {
        super(new UnitActivityContext("DC", depth), depth > 0);
        this.parent = parent;
        this.branch = branch;
        this.depth = depth;
        this.load = load;
    }

    @Override
    public void initialize() throws Exception {

        if (depth == 0) {            

            if (load > 0) { 
                try { 
                    Thread.sleep(load);
                } catch (Exception e) {
                    // ignore
                }
            }

            finish();
        } else {
            for (int i=0;i<branch;i++) { 
                executor.submit(new DivideAndConquerWithSleep(identifier(),
                        branch, depth-1, load));
            }
            suspend();
        } 
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process(Event e) throws Exception {

        count += (Long)((MessageEvent) e).message;

        merged++;

        if (merged < branch) { 
            suspend();
        } else { 
            finish();
        }
    }

    @Override
    public void cleanup() throws Exception {
        executor.send(new MessageEvent(identifier(), parent, count));        
    }

    public String toString() { 
        return "DC(" + identifier() + ") " + branch + ", " + depth + ", " 
        + merged + " -> " + count;
    }

    public static void main(String [] args) { 
        
        try {
            long start = System.currentTimeMillis();

            int branch = Integer.parseInt(args[0]);
            int depth =  Integer.parseInt(args[1]);
            int load =  Integer.parseInt(args[2]);
            int nodes = Integer.parseInt(args[3]);
            int executors = Integer.parseInt(args[4]);
            
            Executor [] e = new Executor[executors];
            
            for (int i=0;i<executors;i++) { 
                e[i] = new SimpleExecutor(new UnitWorkerContext("DC"), StealStrategy.SMALLEST, StealStrategy.BIGGEST);
            }
            
            Constellation c = ConstellationFactory.createConstellation(e);
            c.activate();
            
            if (c.isMaster()) { 

                long count = 0;

                for (int i=0;i<=depth;i++) { 
                    count += Math.pow(branch, i);
                }

                double time = (load * Math.pow(branch, depth)) / (1000*(nodes*executors)); 

                System.out.println("Running D&C with branch factor " + branch + 
                        " and depth " + depth + " load " + load + 
                        " (expected jobs: " + count + ", expected time: " + 
                        time + " sec.)");

                SingleEventCollector a = new SingleEventCollector(new UnitActivityContext("DC"));

                c.submit(a);
                c.submit(new DivideAndConquerWithSleep(a.identifier(), branch, depth, load));
                
                long result = (Long) ((MessageEvent)a.waitForEvent()).message;

                long end = System.currentTimeMillis();

                double nsPerJob = (1000.0*1000.0 * (end-start)) / count;

                String correct = (result == count) ? " (CORRECT)" : " (WRONG!)";

                System.out.println("D&C(" + branch + ", " + depth + ") = " + result + 
                        correct + " total time = " + (end-start) + " job time = " + 
                        nsPerJob + " nsec/job");

            }
            
            c.done();
        
        } catch (Exception e) {
            System.err.println("Oops: " + e);
            e.printStackTrace(System.err);
            System.exit(1);
        }

    }

    @Override
    public void cancel() throws Exception {
        // TODO Auto-generated method stub

    }


}
