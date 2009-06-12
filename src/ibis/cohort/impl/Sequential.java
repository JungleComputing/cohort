package ibis.cohort.impl;

import ibis.cohort.Cohort;
import ibis.cohort.Activity;
import ibis.cohort.Event;
import ibis.cohort.Identifier;
import ibis.cohort.MessageEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

public class Sequential implements Cohort {

    private static class ActivityRecord { 

        static final int INITIALIZING = 1;
        static final int SUPENDED     = 2;
        static final int RUNNABLE     = 3;
        static final int FINISHING    = 4;
        static final int DONE         = 5;
        static final int ERROR        = Integer.MAX_VALUE;
        
        final Activity activity;
        private LinkedList<Event> queue;
        private int state = INITIALIZING;

        ActivityRecord(Activity activity) {
            this.activity = activity;
        }

        void enqueue(Event e) { 

            if (state >= FINISHING) { 
                throw new IllegalStateException("Cannot deliver an event to a finished activity!");
            }

            if (queue == null) { 
                queue = new LinkedList<Event>();
            }

            queue.addLast(e);
        }

        Event dequeue() { 

            if (queue == null || queue.size() == 0) { 
                return null;
            }

            return queue.removeFirst();
        }   

        int pendingEvents() { 

            if (queue == null || queue.size() == 0) { 
                
             //   System.out.println("   PENDING EVENTS 0");
                
                return 0;
            }

            //System.out.println("   PENDING EVENTS " + queue.size());
            
            return queue.size();
        }

        Identifier identifier() { 
            return activity.identifier();
        }
        
        boolean isRunnable() { 
            return (state == RUNNABLE);
        }
        
        boolean isDone() { 
            return (state == DONE);
        }
        
        boolean needsToRun() { 
            return (state == INITIALIZING || state == RUNNABLE || state == FINISHING);
        }
        
        boolean setRunnable()  { 
            
            if (state == RUNNABLE || state == INITIALIZING) { 
                // it's already runnable 
                return false;
            }
            
            if (state == SUPENDED) { 
                // it's runnable now
                state = RUNNABLE;
                return true;
            }
            
            // It cannot be made runnable
            throw new IllegalStateException("INTERNAL ERROR: activity cannot be made runnable!");
        }
        
        void run() {

            try { 
                switch (state) { 

                case INITIALIZING: 
                    
                   // System.out.println("I -> " + activity);
                  //  System.out.println("INIT " + activity);
                    
                    activity.initialize();

                    if (activity.mustSuspend()) { 
                       // System.out.println("   SUSPEND " + activity); 
                        state = SUPENDED;
                    } else if (activity.mustFinish()) { 
                      //  System.out.println("   FINISHING " + activity); 
                        
                        state = FINISHING;
                    } else { 
                        throw new IllegalStateException("Activity did not suspend or finish!");
                    }
                    
                    activity.reset();
                    break;

                case RUNNABLE:

                  //  System.out.println("P -> " + activity);
                    
                 //   System.out.println("PROCESS " + activity);
                    
                    Event e = dequeue();

                    if (e == null) { 
                        throw new IllegalStateException("INTERNAL ERROR: Runnable activity has no pending events!");
                    }

                    activity.process(e);

                    if (activity.mustSuspend()) { 
                        // We only suspend the job if there are no pending events.
                        if (pendingEvents() > 0) { 
                            state = RUNNABLE;
                        //    System.out.println("   RUNNABLE " + activity); 
                        } else {
                            state = SUPENDED;
                        //    System.out.println("   SUSPEND " + activity); 
                        }
                    } else if (activity.mustFinish()) { 
                        //System.out.println("   FINISHING " + activity); 
                        state = FINISHING;
                    } else { 
                        throw new IllegalStateException("Activity did not suspend or finish!");
                    }
                    
                    activity.reset();
                    break;

                case FINISHING: 

                   // System.out.println("F -> " + activity);

                //    System.out.println("FINISH " + activity);
                    
                    activity.cleanup();
                    state = DONE;
                    break;

                case DONE:
                    throw new IllegalStateException("INTERNAL ERROR: Running activity that is already done");

                case ERROR:
                    throw new IllegalStateException("INTERNAL ERROR: Running activity that is in an error state!");
                    
                default:
                    throw new IllegalStateException("INTERNAL ERROR: Running activity with unknown state!");
                }
                
            } catch (Exception e) { 
                System.err.println("Activity failed: " + e);
                e.printStackTrace(System.err);
                state = ERROR;
            }
        }
    }

    private HashMap<Identifier, ActivityRecord> all = 
        new HashMap<Identifier, ActivityRecord>();

    private ArrayList<ActivityRecord> fresh = new ArrayList<ActivityRecord>();    
    private ArrayList<ActivityRecord> runnable = new ArrayList<ActivityRecord>();    

    private boolean isRunning = false;

    private long nextID = 0;
    
    public boolean cancel(Identifier id) {
        
        ActivityRecord ar = all.remove(id);
        
        if (ar == null) { 
            return false;
        } 
        
        //System.out.println("CANCEL " + ar.activity);
        
        if (ar.needsToRun()) { 
            runnable.remove(ar);
        }
        
        return true;
    }
    
    public boolean cancelAll() {

        if (all.size() == 0) { 
            return false;
        }
       
        all.clear();
        runnable.clear();
        
        return true;
    }

    public void done() {
        System.out.println("Quiting Cohort with " + all.size() + " activities in queue");
    }

    private ActivityRecord dequeue() {

        int size = runnable.size(); 

        if (size > 0) { 
            return runnable.remove(size-1);
        }

        size = fresh.size();

        if (size > 0) { 
            return fresh.remove(size-1);
        }

        return null;
    }
    
    private Identifier createActivityID() { 
        return new SequentialIdentifier(nextID++);
    }

    public Identifier submit(Activity a) {
       
        Identifier id = createActivityID();

        a.initialize(this, id);

        ActivityRecord ar = new ActivityRecord(a);
        all.put(a.identifier(), ar);
        fresh.add(ar); 

        // This is a sequential version, so we grab the user thread when a job 
        // is submitted, and don't return until the job has finished. However, 
        // since recusive calls to submit are likely to occur, we must be 
        // a bit carefull here... 
        if (!isRunning) { 
            processJobs();
        }

        return id;
    }

    public void send(Identifier source, Identifier target, Object o) {

        ActivityRecord ar = all.get(target);
        
        if (ar == null) { 

            System.out.println("   SEND FAILED " + source + " -> " + target + " " + o);

            new Exception().printStackTrace(System.out);
            
            System.exit(1);
        }

      //  System.out.println("   SEND " + source + " -> " + target + " (" + ar.activity + ") " + o);
        
        
        ar.enqueue(new MessageEvent(source, o));

        boolean change = ar.setRunnable();
        
        if (change) {     
           // System.out.println(target + " is now runnable");
            runnable.add(ar);
        } else { 
          //  System.out.println(target + " already runnable ?");
        }
    } 

    
    
    private void processJobs() { 

        isRunning = true;
        
        ActivityRecord tmp = dequeue();

        while (tmp != null) {
            tmp.run();

            if (tmp.needsToRun()) { 
                runnable.add(tmp);
            } else if (tmp.isDone()) { 
                cancel(tmp.identifier());
            }
            
            tmp = dequeue();
        }

        isRunning = false;
        
        // sanity check
        if (runnable.size() > 0) { 
            throw new RuntimeException("Quiting while there are runnable activities!");
        }
    }

}
