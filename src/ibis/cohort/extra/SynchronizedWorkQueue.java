package ibis.cohort.extra;

import ibis.cohort.Context;
import ibis.cohort.impl.distributed.ActivityRecord;

public class SynchronizedWorkQueue extends WorkQueue {

    private WorkQueue queue;
    
    public SynchronizedWorkQueue(WorkQueue queue) {
        super(queue.id);
        this.queue = queue;
    }
    
    @Override
    public synchronized ActivityRecord dequeue() {
        return queue.dequeue();
    }

    @Override
    public synchronized void enqueue(ActivityRecord a) {
        queue.enqueue(a);
    }

    @Override
    public synchronized int size() {
        return queue.size();
    }

    @Override
    public synchronized ActivityRecord steal(Context c) {
        return queue.steal(c);
    }

    @Override
    public synchronized void enqueue(ActivityRecord [] a) { 
        queue.enqueue(a);
    }
    
    @Override
    public synchronized ActivityRecord [] dequeue(int count) { 
        return queue.dequeue(count);
    }
    
    @Override
    public synchronized ActivityRecord [] steal(Context c, int count) {
        return queue.steal(c, count);
    }
}
