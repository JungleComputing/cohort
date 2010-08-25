package ibis.cohort.extra;

import ibis.cohort.WorkerContext;
import ibis.cohort.impl.distributed.ActivityRecord;

public abstract class WorkQueue {

    protected final String id; 
    
    protected WorkQueue(String id) { 
        this.id = id;
    }
    
    public abstract void enqueue(ActivityRecord a);
    public abstract ActivityRecord dequeue(boolean head);
    public abstract ActivityRecord steal(WorkerContext c); 
    public abstract int size(); 
    
    protected ActivityRecord [] trim(ActivityRecord [] a, int count) { 
        ActivityRecord [] result = new ActivityRecord[count];
        System.arraycopy(a, 0, result, 0, count);
        return result;
    }
    
    
    public void enqueue(ActivityRecord [] a) { 
        for (int i=0;i<a.length;i++) { 
            enqueue(a[i]);
        }
    }
    
    public ActivityRecord [] dequeue(int count, boolean head) { 
        
        ActivityRecord [] tmp = new ActivityRecord[count];
        
        for (int i=0;i<count;i++) { 
            tmp[i] = dequeue(head);
            
            if (tmp[i] == null) { 
                return trim(tmp, i);
            }
        }
        
        return tmp;
    }
    
    public ActivityRecord [] steal(WorkerContext c, int count) {
        
        ActivityRecord [] tmp = new ActivityRecord[count];
        
        for (int i=0;i<count;i++) { 
            tmp[i] = steal(c);
            
            if (tmp[i] == null) { 
                return trim(tmp, i);
            }
        }
        
        return tmp;
    }
}
