package ibis.cohort.extra;

import ibis.cohort.Context;
import ibis.cohort.context.ContextSet;
import ibis.cohort.context.UnitContext;
import ibis.cohort.impl.distributed.ActivityRecord;

import java.util.HashMap;

public class SmartWorkQueue extends WorkQueue {

    // We maintain three lists here, which reflect the relative complexity of 
    // the context associated with the jobs: 
    //
    // 'Any' jobs are easy to place
    // 'UNIT or AND' jobs are likely to have limited suitable locations
    // 'OR' jobs have more suitable locations
    
    protected final CircularBuffer any = new CircularBuffer(1);
   
    protected final HashMap<Context, CircularBuffer> unitAnd = 
        new HashMap<Context, CircularBuffer>();
      
    protected final HashMap<Context, CircularBuffer> or = 
        new HashMap<Context, CircularBuffer>();
    
    protected int size;
    
    @Override
    public int size() {
        return size;
    }
    
    private ActivityRecord getUnitAnd(Context c) { 

        CircularBuffer tmp = unitAnd.get(c);
        
        if (tmp == null) { 
            return null;
        }
        
        ActivityRecord a = (ActivityRecord) tmp.removeLast();
        
        if (tmp.size() == 0) { 
            unitAnd.remove(c);
        }
        
        size--;
        
        return a;
    }
    
    private ActivityRecord getOr(Context c) { 
        
        if (c.isSet()) { 
            c = ((ContextSet) c).getContexts()[0];
        }
        
        CircularBuffer tmp = or.get(c);

        if (tmp == null) { 
            return null;
        }

        ActivityRecord a = (ActivityRecord) tmp.removeLast();

        Context [] all = ((ContextSet) a.activity.getContext()).getContexts();
        
        for (int i=0;i<all.length;i++) { 

            // Remove this activity from all entries in the 'or' table
            tmp = or.get(all[i]);

            if (tmp != null) { 
                tmp.remove(a);

                if (tmp.size()== 0) { 
                    or.remove(all[i]);
                }
            }
        }

        size--;
        return a;
    }
    
    @Override
    public ActivityRecord dequeue() {
    
        if (size == 0) { 
            return null;
        }
    
        if (unitAnd.size() > 0) {
            return getUnitAnd(unitAnd.keySet().iterator().next());
        }
    
        if (or.size() > 0) { 
            return getOr(or.keySet().iterator().next());
        } 
    
        if (any.size() > 0) { 
            size--;
            return (ActivityRecord) any.removeLast();
        }
    
        return null;
    }

    // NOTE: only works for Unit and and contexts
    private void enqueueUnitAnd(Context c, ActivityRecord a) {
 
        CircularBuffer tmp = unitAnd.get(c);
        
        if (tmp == null) { 
            tmp = new CircularBuffer(1);
            unitAnd.put(c, tmp);
        }
    
        tmp.insertLast(a);
        size++;
    }
 
    // NOTE: only works for Unit and and contexts
    private void enqueueOr(ContextSet c, ActivityRecord a) {
 
        Context [] all = ((ContextSet) c).getContexts();
        
        for (int i=0;i<all.length;i++) { 

            CircularBuffer tmp = or.get(all[i]);
           
            if (tmp == null) { 
                tmp = new CircularBuffer(1);
                or.put(all[i], tmp);
            }
            
            tmp.insertLast(a);
        }
        
        size++;
    }
 
    
    @Override
    public void enqueue(ActivityRecord a) {
    
        Context c = a.activity.getContext();
        
        if (c.isAny()) { 
            any.insertLast(a);
            size++;
            return;
        }
        
        if (c.isUnit() || c.isAnd()) {
            enqueueUnitAnd((UnitContext) c, a);
            return;
        }
        
        if (c.isSet()) {
            enqueueOr((ContextSet) c, a);
            return;
        }
    
        System.err.println("EEP: ran into unknown Context! " + c);
    }

    @Override
    public ActivityRecord steal(Context c) {
       
        if (c.isUnit() || c.isAnd()) { 
        
            ActivityRecord a = getUnitAnd(c);
            
            if (a == null) { 
                a = getOr(c);
            
                if (a == null && any.size() > 0) { 
                    a = (ActivityRecord) any.removeFirst();
                }
            }
            
            return a;
        }
        
        if (c.isSet()) { 
            
            Context [] and = ((ContextSet) c).andContexts();
            
            if (and != null && and.length > 0) { 
                for (int i=0;i<and.length;i++) {
                    ActivityRecord a = getUnitAnd(c);
                    
                    if (a != null) { 
                        return a;
                    } 
 
                    a = getOr(c);
                    
                    if (a != null) { 
                        return a;
                    } 
                }
            } 
            
            Context [] unit = ((ContextSet) c).unitContexts();
            
            if (unit != null && unit.length > 0) { 
                for (int i=0;i<unit.length;i++) {
                    ActivityRecord a = getUnitAnd(c);
                    
                    if (a != null) { 
                        return a;
                    } 
 
                    a = getOr(c);
                    
                    if (a != null) { 
                        return a;
                    } 
                }
            } 
        
            if (any.size() > 0) { 
                return (ActivityRecord) any.removeFirst();
            }
        }
   
        // NOT SURE WHAT TO DO WITH ANY!
        return null;
    }
}
