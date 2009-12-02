package ibis.cohort.impl.multithreaded;

import java.io.PrintStream;
import java.util.LinkedList;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Cohort;
import ibis.cohort.CohortIdentifier;
import ibis.cohort.Context;
import ibis.cohort.Event;

public class MTCohort implements Cohort {

    private LinkedList<ActivityRecord> available = new LinkedList<ActivityRecord>();
    
    private final MTCohortIdentifier identifier;
    
    private STCohort [] workers;
    private int nextSubmit = 0;
    
    private long startID = 0;
    private long blockSize = 1000000;
    
    public MTCohort(int workerCount) { 
    
        identifier = new MTCohortIdentifier(Integer.MAX_VALUE);
        
        if (workerCount == 0) { 

            String tmp = System.getProperty("ibis.cohort.workers");

            if (tmp != null && tmp.length() > 0) { 
                try { 
                    workerCount = Integer.parseInt(tmp);
                } catch (Exception e) {
                    System.err.println("Failed to parse property ibis.cohort.workers: " + e);
                }
            }

            if (workerCount == 0) { 
                // Automatically determine the number of cores to use
                workerCount = Runtime.getRuntime().availableProcessors();
            }
        }
        
        workers = new STCohort[workerCount];
       
        for (int i=0;i<workerCount;i++) { 
            workers[i] = new STCohort(this, new MTCohortIdentifier(i));
        }
    
        for (int i=0;i<workerCount;i++) { 
            new Thread(workers[i], "Cohort ComputationUnit " + i).start();
        }
    }
    
    public PrintStream getOutput() {
        return System.out;
    }
    
    
    public void cancel(ActivityIdentifier activity) {
        for (STCohort u : workers) { 
            u.cancel(activity);
        }
    }

    public void done() {
        for (STCohort u : workers) { 
            u.done();
        }
    }

    public synchronized ActivityIdentifier submit(Activity a) {
 
        // We do a simple round-robin distribution of the jobs here.
        if (nextSubmit >= workers.length) { 
            nextSubmit = 0;
        }
        
        return workers[nextSubmit++].submit(a);
    }

    public synchronized IDGenerator getIDGenerator() {
        
      //  System.out.println("New IDGenerator(" + startID +")");
        
        IDGenerator tmp = new IDGenerator(startID, startID+blockSize);
        startID += blockSize;
        return tmp;
    }

    public void send(ActivityIdentifier source, ActivityIdentifier target, Object o) {
        
        // TODO: improve this incredibly dumb implementation!
 
        workers[0].send(source, target, o);
    }
    
    void forwardEvent(Event e, CohortIdentifier src) { 
 
        // TODO: improve this incredibly dumb implementation!
        
        int source = ((MTCohortIdentifier) src).getIdentifier();
        
        int next = (source + 1) % workers.length;
        
        if (next == source) { 
            System.err.println("FAILED TO DELIVER EVENT! " + e);
            System.exit(1);
        } else { 
            workers[next].queueEvent(e);
        }
    }

    // TODO: improve stealing ?
    synchronized void stealReply(ActivityRecord record) {
        
        if (record == null) { 
            System.out.println("EEP: steal reply is null!!");
            new Exception().printStackTrace();
        }
        
        available.addLast(record);
    }    

    ActivityRecord stealAttempt(CohortIdentifier src) {
    
        int source = ((MTCohortIdentifier) src).getIdentifier();
        
        synchronized (this) {
            if (available.size() > 0) { 
                return available.removeFirst();
            }
        }

        for (int i=0;i<workers.length;i++) { 
            if (i != source) { 
                workers[i].stealRequest();
            }
        }

        return null;
    }

    public CohortIdentifier identifier() {
        return identifier;
    }

    public boolean isMaster() {
        return true;
    }

    public Context getContext() {
        // TODO Auto-generated method stub
        return null;
    }

    public void setContext(Context context) {
        // TODO Auto-generated method stub
        
    }

    public Cohort[] getSubCohorts() {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean activate() {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean deregister(String name, Context scope) {
        // TODO Auto-generated method stub
        return false;
    }

    public ActivityIdentifier lookup(String name, Context scope) {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean register(String name, ActivityIdentifier id, Context scope) {
        // TODO Auto-generated method stub
        return false;
    }

    public void clearContext() {
        // TODO Auto-generated method stub
        
    }
}
