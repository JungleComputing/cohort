package ibis.cohort.impl.distributed;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Event;

import java.util.LinkedList;

class ActivityRecord { 

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
            throw new IllegalStateException("Cannot deliver an event to a finished activity! " + activity + " (event from " + e.source + ")");
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

    ActivityIdentifier identifier() { 
        return activity.identifier();
    }

    boolean isRunnable() { 
        return (state == RUNNABLE);
    }

    boolean isDone() { 
        return (state == DONE);
    }

    public boolean isFresh() {
        return (state == INITIALIZING);
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

               //  System.out.println("I -> " + activity);
      //           System.out.println("INIT " + activity);

                activity.initialize();

                if (activity.mustSuspend()) { 
                    if (pendingEvents() > 0) { 
                        state = RUNNABLE;
                //            System.out.println("   RUNNABLE " + activity); 
                    } else {
                        state = SUPENDED;
        //                    System.out.println("   SUSPEND " + activity); 
                    }
            
                } else if (activity.mustFinish()) { 
          //            System.out.println("   FINISHING " + activity); 

                    state = FINISHING;
                } else { 
                    throw new IllegalStateException("Activity did not suspend or finish!");
                }

                activity.reset();
                break;

            case RUNNABLE:

                //  System.out.println("P -> " + activity);

//                   System.out.println("PROCESS " + activity);

                Event e = dequeue();

                if (e == null) { 
                    throw new IllegalStateException("INTERNAL ERROR: Runnable activity has no pending events!");
                }

                activity.process(e);

                if (activity.mustSuspend()) { 
                    // We only suspend the job if there are no pending events.
                    if (pendingEvents() > 0) { 
                        state = RUNNABLE;
//                              System.out.println("   RUNNABLE " + activity); 
                    } else {
                        state = SUPENDED;
//                            System.out.println("   SUSPEND " + activity); 
                    }
                } else if (activity.mustFinish()) { 
//                    System.out.println("   FINISHING " + activity); 
                    state = FINISHING;
                } else { 
                    throw new IllegalStateException("Activity did not suspend or finish!");
                }

                activity.reset();
                break;

            case FINISHING: 

                // System.out.println("F -> " + activity);

//                     System.out.println("FINISH " + activity);

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
    
    private String getStateAsString() { 
        
        switch (state) { 

            case INITIALIZING:
                return "initializing";
            case SUPENDED:
                return "suspended";            
            case RUNNABLE:
                return "runnable";
            case FINISHING: 
                return "finishing";
            case DONE:
                return "done";
            case ERROR:
                return "error";
        }

        return "unknown";
    }
    
    public String toString() { 
     
        return activity + " STATE: " + getStateAsString();
        
    }    
}
