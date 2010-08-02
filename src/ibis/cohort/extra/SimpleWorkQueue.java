package ibis.cohort.extra;

import ibis.cohort.Context;
import ibis.cohort.impl.distributed.ActivityRecord;

public class SimpleWorkQueue extends WorkQueue {

    // This is a very simple workqueue implementation. It has the following 
    // properties: 
    //
    // dequeue returns work in LIFO order
    // steal returns work in FIFO order
    // steal does not take Context complexity into account. It simply returns 
    //    the first job that matches. 
    // there is an optimized version of steal(Context, count) available.
    
    protected final CircularBuffer buffer = new CircularBuffer(1);
    
    @Override
    public int size() {
        return buffer.size();
    }
    
    @Override
    public ActivityRecord dequeue() {
    
        if (buffer.empty()) { 
            return null;
        }
        
        return (ActivityRecord) buffer.removeLast();
    }

    @Override
    public void enqueue(ActivityRecord a) {
        buffer.insertLast(a);
    }

    @Override
    public ActivityRecord steal(Context c) {
    
        if (buffer.empty()) { 
            return null;
        }
        
        for (int i=0;i<buffer.size();i++) { 
            
            Context tmp = ((ActivityRecord) buffer.get(i)).activity.getContext();
            
            if (tmp.satisfiedBy(c)) { 
                ActivityRecord a = (ActivityRecord) buffer.get(i);
                buffer.remove(i);
                return a;
            }
   
        }
        
        return null;
    }
}
