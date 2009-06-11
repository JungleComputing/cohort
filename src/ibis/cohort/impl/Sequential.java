package ibis.cohort.impl;

import ibis.cohort.Cohort;
import ibis.cohort.Job;
import ibis.cohort.JobIdentifier;
import ibis.cohort.ResultHandler;
import ibis.cohort.SimpleResultHandler;

import java.util.ArrayList;

public class Sequential implements Cohort {

    private ArrayList<Job> fresh = new ArrayList<Job>();    
    private ArrayList<Job> runnable = new ArrayList<Job>();    
    private ArrayList<Job> suspended = new ArrayList<Job>();

    private boolean isRunning = false;

    private boolean cancel(ArrayList<Job> list, JobIdentifier id) { 

        for (int i=0;i<list.size();i++) { 

            Job tmp = list.get(i);

            if (tmp.identifier().equals(id)) { 
                list.remove(i);
                return true;
            }
        }

        return false;
    }

    public boolean cancel(JobIdentifier id) {
        return cancel(fresh, id) || cancel(runnable, id) 
            || cancel(suspended, id);
    }

    public boolean cancelAll() {

        if (fresh.size() == 0 && runnable.size() == 0 
                && suspended.size() == 0) { 
            return false;
        }

        fresh.clear();
        runnable.clear();
        suspended.clear();

        return true;
    }

    public void done() {
        // ignored
    }

    public void finished(Job job, Object result) {
        job.getResultHandler().storeResult(job, result);
    }

    private void enqueue(Job job) {  
        runnable.add(job);
    }

    private Job dequeue() {

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

    public JobIdentifier submit(Job job, ResultHandler h) {

        job.setCohort(this);
        job.setResultHandler(h);
        job.submitted();
        
        enqueue(job);

        // This is a sequential version, so we grab the user thread when a job 
        // is submitted, and don't return until the job has finished. However, 
        // since recusive calls to submit are likely to occur, we must be 
        // carefull here... 
        if (!isRunning) { 
            processJobs();
        }

        return job.identifier();
    }

    public Object submit(Job job) {

        SimpleResultHandler rh = new SimpleResultHandler();

        submit(job, rh);

        return rh.waitForResult();
    }

    public void unsuspend(Job job) { 
        
        if (!job.isSuspended()) { 
            return;
        }
        
        if (suspended.remove(job)) {
            job.setRunnable(true);
            runnable.add(job);            
        }
    }
    
    private void processJobs() { 

        Job tmp = dequeue();

        while (tmp != null) { 

            if (tmp.isSuspended()) {
                throw new RuntimeException("Unexpectedly got unrunnable job!");
            }

            do { 

                try { 
                    tmp.run();
                } catch (Exception e) { 
                    throw new RuntimeException("Unexpected exception", e);
                }

            } while (!tmp.isDone() && !tmp.isSuspended());

            if (!tmp.isDone()) {
                // Job has simply suspended
                suspended.add(tmp);
            }

            tmp = dequeue();
        }

        // sanity check
        if (runnable.size() > 0) { 
            throw new RuntimeException("EEP1");
        }

        if (suspended.size() > 0) { 
            throw new RuntimeException("EEP2");
        }        
    } 
}
