package test.lowlevel;

import ibis.cohort.Activity;
import ibis.cohort.Cohort;
import ibis.cohort.CohortFactory;
import ibis.cohort.CohortIdentifier;
import ibis.cohort.Context;
import ibis.cohort.Event;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.MessageEvent;
import ibis.cohort.SingleEventCollector;
import ibis.cohort.context.UnitContext;

public class DivideAndConquerWithContextAndPenalty extends Activity {

    /*
     * This is a simple divide and conquer example. The user can specify the branch factor 
     * and tree depth on the command line. All the application does is calculate the sum of 
     * the number of nodes in each subtree. 
     */

    private static final long serialVersionUID = 3379531054395374984L;

    private static final int CONTEXT_NONE   = 0;
    private static final int CONTEXT_WEAK   = 1;
    private static final int CONTEXT_STRONG = 2;

    private static final int TYPE_LEFT_RIGHT      = 0;
    private static final int TYPE_LEAF_LEFT_RIGHT = 1;
    private static final int TYPE_ZEBRA           = 2;
    private static final int TYPE_RANDOM          = 3;
    private static final int TYPE_LEAF_RANDOM     = 4;
    
    private final ActivityIdentifier parent;

    private final int branch;
    private final int depth;
    private final int sleep;
    private final int penalty;
    private final int mode;
    private final int type;
        
    private int merged = 0;    
    private long count = 1;

    public DivideAndConquerWithContextAndPenalty(ActivityIdentifier parent, 
            int branch, int depth, int sleep, int penalty, int mode, int type,  
            Context context) {
      
        super(context);
        this.parent = parent;
        this.branch = branch;
        this.depth = depth;
        this.sleep = sleep;
        this.penalty = penalty;
        this.mode = mode;
        this.type = type;
    }

    @Override
    public void initialize() throws Exception {

        if (depth == 0) {            

            long time = sleep;
        
            // With WEAK context each machine can run each job, but there 
            // is a performance penalty when the context doesn't match.
            // With NONE or STRONG context we either don't care about 
            // context, or cohort has ensured the context is correct. Either 
            // way, just sleep for the specified amount of time.
            
            if (mode == CONTEXT_WEAK) { 
                
                Context machineContext = getCohort().getContext();
                Context activitycontext = getContext();
                    
                if (machineContext == null || machineContext.equals(UnitContext.DEFAULT)) { 
                    
                    // Check if context stored in LocalData is same as activity 
                    // context. If not, add penalty to time.
                    machineContext = (Context) LocalData.getLocalData().get("context");
                    
                    if (!activitycontext.equals(machineContext)) { 
                        time = time + penalty;
                
                        Integer tmp = (Integer) LocalData.getLocalData().get("mismatch");
                        LocalData.getLocalData().put("mismatch", new Integer(tmp.intValue()+1));
                        //System.out.println("WEAK performace hit!");
                    }// else { 
                        //System.out.println("WEAK no performace hit");
                    //}
                }
            } 
            
            if (time > 0) { 
                try { 
                    Thread.sleep(time);
                } catch (Exception e) {
                    // ignore
                }
            }
            
            finish();
        } else {

            if (type == TYPE_LEFT_RIGHT) { 
                Context even = new UnitContext("Even");
                Context odd = new UnitContext("Odd");
                
                for (int i=0;i<branch;i++) { 
                    Context tmp = (i % 2) == 0 ? even : odd;
                    cohort.submit(new DivideAndConquerWithContextAndPenalty(
                            identifier(), branch, depth-1, sleep, penalty, mode, 
                            type, tmp));
                }
            } else if (type == TYPE_LEAF_LEFT_RIGHT) { 
          
                if (depth == 1) { 
                    Context even = new UnitContext("Even");
                    Context odd = new UnitContext("Odd");
                
                    for (int i=0;i<branch;i++) { 
                        Context tmp = (i % 2) == 0 ? even : odd;
                        cohort.submit(new DivideAndConquerWithContextAndPenalty(
                                identifier(), branch, depth-1, sleep, penalty, mode, 
                                type, tmp));
                    }
                } else { 
                    for (int i=0;i<branch;i++) { 
                        cohort.submit(new DivideAndConquerWithContextAndPenalty(
                                identifier(), branch, depth-1, sleep, penalty, mode, 
                                type, UnitContext.DEFAULT));
                    }
                }
                
            } else if (type == TYPE_ZEBRA) { 
          
                Context tmp = null; 
                
                if (depth % 2 == 0) { 
                    tmp = new UnitContext("Even");
                } else { 
                    tmp = new UnitContext("Odd");
                }
                
                for (int i=0;i<branch;i++) { 
                    cohort.submit(new DivideAndConquerWithContextAndPenalty(
                            identifier(), branch, depth-1, sleep, penalty, mode, 
                            type, tmp));
                }
                
            } else if (type == TYPE_RANDOM) { 

                Context even = new UnitContext("Even");
                Context odd = new UnitContext("Odd");
            
                for (int i=0;i<branch;i++) { 
            
                    Context tmp = null;
                    
                    if (Math.random() > 0.5) { 
                        tmp = even;
                    } else { 
                        tmp = odd;
                    }
                    
                    cohort.submit(new DivideAndConquerWithContextAndPenalty(
                            identifier(), branch, depth-1, sleep, penalty, mode, 
                            type, tmp));
                }
                
            } else if (type == TYPE_LEAF_RANDOM) { 

                if (depth == 1) { 
                    Context even = new UnitContext("Even");
                    Context odd = new UnitContext("Odd");
                
                    for (int i=0;i<branch;i++) { 
                
                        Context tmp = null;
                        
                        if (Math.random() > 0.5) { 
                            tmp = even;
                        } else { 
                            tmp = odd;
                        }
                        
                        cohort.submit(new DivideAndConquerWithContextAndPenalty(
                                identifier(), branch, depth-1, sleep, penalty, mode, 
                                type, tmp));
                    }
                } else { 
                    for (int i=0;i<branch;i++) { 
                        cohort.submit(new DivideAndConquerWithContextAndPenalty(
                                identifier(), branch, depth-1, sleep, penalty, mode, 
                                type, UnitContext.DEFAULT));
                    }
                }
            }
            
            suspend();
        } 
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process(Event e) throws Exception {

        count += ((MessageEvent<Long>) e).message;

        merged++;

        if (merged < branch) { 
            suspend();
        } else { 
            finish();
        }
    }

    @Override
    public void cleanup() throws Exception {
        cohort.send(identifier(), parent, count);        
    }

    @Override
    public void cancel() throws Exception {
        // TODO Auto-generated method stub

    }
    
    public String toString() { 
        return "DC(" + identifier() + ") " + branch + ", " + depth + ", " 
        + merged + " -> " + count;
    }

    private static int parseMode(String context) { 
        
        if (context.equals("none")) { 
           return CONTEXT_NONE;
        } else if (context.equals("weak")) { 
           return CONTEXT_WEAK;
        } else if (context.equals("strong")) { 
           return CONTEXT_STRONG;
        } 
        
        System.err.println("Unknown context mode: " + context);
        System.exit(1);
        
        // Stupid compiler 
        return -1;
    }

    private static int parseType(String type) { 
        
        if (type.equals("left-right")) { 
           return TYPE_LEFT_RIGHT;
        } else if (type.equals("leaf-left-right")) { 
           return TYPE_LEAF_LEFT_RIGHT;
        } else if (type.equals("zebra")) { 
           return TYPE_ZEBRA;
        } else if (type.equals("random")) {
            return TYPE_RANDOM;
        } else if (type.equals("leaf-random")) {
            return TYPE_LEAF_RANDOM;
        }
        
        System.err.println("Unknown type: " + type);
        System.exit(1);
        
        // Stupid compiler 
        return -1;
    }

    public static void main(String [] args) { 
        
        try {
            long start = System.currentTimeMillis();

            Cohort cohort = CohortFactory.createCohort();
               
            int branch = Integer.parseInt(args[0]);
            int depth =  Integer.parseInt(args[1]);
            int sleep =  Integer.parseInt(args[2]);
            int penalty =  Integer.parseInt(args[3]);
            int workers = Integer.parseInt(args[4]);
            int rank = Integer.parseInt(args[5]);
            
            CohortIdentifier [] leafs = cohort.getLeafIDs();

            Context cohortContext = null;
   
            int mode = parseMode(args[6]);            
            int type = parseType(args[7]);         
            
            if (mode == CONTEXT_NONE) { 
                cohortContext = UnitContext.DEFAULT;
            } else if (mode == CONTEXT_WEAK) { 
                cohortContext = UnitContext.DEFAULT;
   
                Context local = null;
                
                if (rank % 2 == 0) { 
                    local = new UnitContext("Even");
                } else { 
                    local = new UnitContext("Odd");
                }
                
                System.out.println("LocalData context set to " + local);
                LocalData.getLocalData().put("context", local);
                LocalData.getLocalData().put("mismatch", new Integer(0));
                
            } else if (mode == CONTEXT_STRONG) { 
                if (rank % 2 == 0) { 
                    cohortContext = new UnitContext("Even");
                } else { 
                    cohortContext = new UnitContext("Odd");
                }
            }
            
            System.out.println("Setting cohort context to " + cohortContext);
            
            for (CohortIdentifier id : leafs) { 
                cohort.setContext(id, cohortContext);
            }
    
            cohort.activate();
            
            if (cohort.isMaster()) { 
           
                long count = 0;

                for (int i=0;i<=depth;i++) { 
                    count += Math.pow(branch, i);
                }

                double min = (sleep * Math.pow(branch, depth)) / (1000*workers); 
                double max = ((sleep + penalty) * Math.pow(branch, depth)) / (1000*workers); 
                
                System.out.println("Running D&C with branch factor " + branch + 
                        " and depth " + depth + " sleep " + sleep + 
                        " (expected jobs: " + count + ", expected time: " + 
                        min  + " ~ " + max + " sec.)");

                SingleEventCollector a = new SingleEventCollector();

                cohort.submit(a);
                cohort.submit(new DivideAndConquerWithContextAndPenalty(
                        a.identifier(), branch, depth, sleep, penalty, mode, 
                        type, new UnitContext("Even")));

                long result = ((MessageEvent<Long>)a.waitForEvent()).message;

                long end = System.currentTimeMillis();

                double nsPerJob = (1000.0*1000.0 * (end-start)) / count;

                String correct = (result == count) ? " (CORRECT)" : " (WRONG!)";

                System.out.println("D&C(" + branch + ", " + depth + ") = " + result + 
                        correct + " total time = " + (end-start) + " job time = " + 
                        nsPerJob + " nsec/job");
            }
                    
            cohort.done();
        
            if (mode == CONTEXT_WEAK) { 
                Integer tmp = (Integer) LocalData.getLocalData().get("mismatch");
                System.out.println("Mismatched jobs = " + tmp);
            }

        } catch (Exception e) {
            System.err.println("Oops: " + e);
            e.printStackTrace(System.err);
            System.exit(1);
        }

    }

    

}
