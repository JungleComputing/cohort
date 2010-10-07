package ibis.constellation.extra;

import ibis.constellation.ActivityIdentifier;
import ibis.constellation.WorkerContext;
import ibis.constellation.impl.distributed.ActivityRecord;

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
    public synchronized ActivityRecord steal(WorkerContext c) {
        return queue.steal(c);
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
    public synchronized ActivityRecord [] steal(WorkerContext c, int count) {
        return queue.steal(c, count);
    }

	@Override
	public boolean contains(ActivityIdentifier id) {
		return queue.contains(id);
	}
	
	@Override
	public ActivityRecord lookup(ActivityIdentifier id) {
		return queue.lookup(id);
	}
}
