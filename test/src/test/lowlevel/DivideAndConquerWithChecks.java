package test.lowlevel;

import ibis.constellation.Activity;
import ibis.constellation.ActivityIdentifier;
import ibis.constellation.Constellation;
import ibis.constellation.ConstellationFactory;
import ibis.constellation.Event;
import ibis.constellation.MessageEvent;
import ibis.constellation.SimpleExecutor;
import ibis.constellation.SingleEventCollector;
import ibis.constellation.StealStrategy;
import ibis.constellation.context.UnitActivityContext;
import ibis.constellation.context.UnitWorkerContext;

import java.util.Arrays;

public class DivideAndConquerWithChecks extends Activity {

    /*
     * This is a simple divide and conquer example. The user can specify the branch factor 
     * and tree depth on the command line. All the application does is calculate the sum of 
     * the number of nodes in each subtree. 
     */
    
    private static final long serialVersionUID = 3379531054395374984L;

    private final ActivityIdentifier parent;

    private final int branch;
    private final int depth;
    
    private int merged = 0;
    
    private long count = 1;
    
    private boolean done = false;
    
    private ActivityIdentifier [] children;
    private ActivityIdentifier [] received;
    
    public DivideAndConquerWithChecks(ActivityIdentifier parent, int branch, int depth) {
        super(UnitActivityContext.DEFAULT, true);
        this.parent = parent;
        this.branch = branch;
        this.depth = depth;
    }

    @Override
    public void initialize() throws Exception {

        if (depth == 0) {
            finish();
        } else {
            
            if (children != null) { 
                System.out.println("EEP: initialize called twice !!!");
            }
            
            children = new ActivityIdentifier[branch];
            received = new ActivityIdentifier[branch];
            
            for (int i=0;i<branch;i++) { 
                children[i] = executor.submit(new DivideAndConquerWithChecks(identifier(), branch, depth-1));
            }
            suspend();
        } 
    }

    private void checkSource(Event e) { 
        
        if (children == null) { 
            System.out.println("EEP: leaf node " + identifier() + " got stray message! " + e.source + " " + e.target);
        }
        
        for (ActivityIdentifier a : children) { 
            if (a.equals(e.source)) { 
                return;
            }
        }
        
        System.out.println("EEP: node " + identifier() + " got stray message! " + e.source + " " + e.target + " " + Arrays.toString(children));
        
        
    }
    
    @Override
    public void process(Event e) throws Exception {
        
        checkSource(e);

        received[merged] = e.source;
        
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
        
        if (!done) { 
            executor.send(new MessageEvent(identifier(), parent, count));
            done = true;
        } else { 
            System.out.println("EEP! Cleanup called twice!");
            new Exception().printStackTrace();
        }
    }
    
    public String toString() { 
        return "DC(" + identifier() + " " + Arrays.toString(children) + " " 
            + Arrays.toString(received) + ") " + branch + ", " + depth + ", " 
            + merged + " -> " + count;
    }

    public static void main(String [] args) throws Exception { 

        long start = System.currentTimeMillis();

        
        Constellation constellation = ConstellationFactory.createConstellation(new SimpleExecutor(UnitWorkerContext.DEFAULT, StealStrategy.SMALLEST, StealStrategy.BIGGEST));
        constellation.activate();
        
        int index = 0;
        
        int branch = Integer.parseInt(args[index++]);
        int depth =  Integer.parseInt(args[index++]);
        
        long count = 0;
        
        for (int i=0;i<=depth;i++) { 
           count += Math.pow(branch, i);
        }
        
        System.out.println("Running D&C with branch factor " + branch + " and depth " 
                + depth + " (expected jobs: " + count + ")");
        
        SingleEventCollector a = new SingleEventCollector();

        constellation.submit(a);
        constellation.submit(new DivideAndConquerWithChecks(a.identifier(), branch, depth));

        long result = (Long)((MessageEvent)a.waitForEvent()).message;

        long end = System.currentTimeMillis();

        double nsPerJob = (1000.0*1000.0 * (end-start)) / count;
        
        String correct = (result == count) ? " (CORRECT)" : " (WRONG!)";
        
        System.out.println("D&C(" + branch + ", " + depth + ") = " + result + 
                correct + " total time = " + (end-start) + " job time = " + nsPerJob + " nsec/job");

        constellation.done();

    }

    @Override
    public void cancel() throws Exception {
        // TODO Auto-generated method stub
        
    }


}
