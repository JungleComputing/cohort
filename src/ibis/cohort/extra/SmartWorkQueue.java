package ibis.cohort.extra;

import ibis.cohort.Context;
import ibis.cohort.context.OrContext;
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
    
    protected final HashMap<Context, CircularBuffer> unitAnd = 
        new HashMap<Context, CircularBuffer>();
      
    protected final HashMap<Context, CircularBuffer> or = 
        new HashMap<Context, CircularBuffer>();
    
    protected int size;
    
    public SmartWorkQueue(String id) { 
        super(id);
    }
    
    @Override
    public int size() {
        return size;
    }
    
    private ActivityRecord getUnitAnd(Context c) { 

        System.out.println("SMART " + id + " getUnitAnd " + c);
        
        CircularBuffer tmp = unitAnd.get(c);
        
        if (tmp == null) { 
            System.out.println("SMART " + id + " getUnitAnd empty!");
            return null;
        }
        
        ActivityRecord a = (ActivityRecord) tmp.removeLast();
       
        System.out.println("SMART " + id + " getUnitAnd returns " + a.identifier());
       
        if (tmp.size() == 0) { 
            unitAnd.remove(c);
        }
        
        size--;
        
        return a;
    }
    
    private ActivityRecord getOr(Context c) { 
        
        System.out.println("SMART " + id + " getOr " + c);
        
        if (c.isOr()) { 
            c = ((OrContext) c).getContexts()[0];
        }
              
        CircularBuffer tmp = or.get(c);

        if (tmp == null) { 
            return null;
        }

        ActivityRecord a = (ActivityRecord) tmp.removeLast();

        Context [] all = ((OrContext) a.activity.getContext()).getContexts();
        
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

        System.out.println("SMART " + id + " getOr returns " + a.identifier() + " " + a.activity.getContext());
        
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
    private void enqueueOr(OrContext c, ActivityRecord a) {
 
        Context [] all = ((OrContext) c).getContexts();
        
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
        
        if (c.isUnit() || c.isAnd()) {
            enqueueUnitAnd((UnitContext) c, a);
            return;
        }
        
        if (c.isOr()) {
            enqueueOr((OrContext) c, a);
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
            }
            
            return a;
        }
        
        if (c.isOr()) { 
            
            Context [] and = ((OrContext) c).andContexts();
            
            if (and != null && and.length > 0) { 
                for (int i=0;i<and.length;i++) {
                    ActivityRecord a = getUnitAnd(and[i]);
                    
                    if (a != null) { 
                        return a;
                    } 
 
                    a = getOr(c);
                    
                    if (a != null) { 
                        return a;
                    } 
                }
            } 
            
            Context [] unit = ((OrContext) c).unitContexts();
            
            if (unit != null && unit.length > 0) { 
                for (int i=0;i<unit.length;i++) {
                    ActivityRecord a = getUnitAnd(unit[i]);
                    
                    if (a != null) { 
                        return a;
                    } 
 
                    a = getOr(c);
                    
                    if (a != null) { 
                        return a;
                    } 
                }
            } 
        }
   
        return null;
    }
}
