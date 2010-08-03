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
    public synchronized ActivityRecord dequeue(boolean head) {
        return queue.dequeue(head);
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
    public synchronized ActivityRecord steal(Context c, boolean head) {
        return queue.steal(c, head);
    }

    @Override
    public synchronized void enqueue(ActivityRecord [] a) { 
        queue.enqueue(a);
    }
    
    @Override
    public synchronized ActivityRecord [] dequeue(int count, boolean head) { 
        return queue.dequeue(count, head);
    }
    
    @Override
    public synchronized ActivityRecord [] steal(Context c, int count, boolean head) {
        return queue.steal(c, count, head);
    }
}
