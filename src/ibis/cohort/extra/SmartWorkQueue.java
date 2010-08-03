package ibis.cohort.extra;

import ibis.cohort.Context;
import ibis.cohort.context.OrContext;
import ibis.cohort.context.UnitContext;
import ibis.cohort.impl.distributed.ActivityRecord;

import java.util.HashMap;

public class SmartWorkQueue extends WorkQueue {

    // We maintain two lists here, which reflect the relative complexity of 
    // the context associated with the jobs: 
    //
    // 'UNIT or AND' jobs are likely to have limited suitable locations, but 
    //     their context matching is easy
    // 'OR' jobs have more suitable locations, but their context matching may be 
    //     expensive
    
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
    
    private ActivityRecord getUnitAnd(Context c, boolean head) { 

     //   System.out.println(System.currentTimeMillis() +" SMART " + id + " getUnitAnd " + c);
        
        CircularBuffer tmp = unitAnd.get(c);
        
        if (tmp == null) { 
           // System.out.println(System.currentTimeMillis() + " SMART " + id + " getUnitAnd empty!");
            return null;
        }
        
        ActivityRecord a; 
        
        if (head) { 
            a = (ActivityRecord) tmp.removeFirst();
        } else { 
            a = (ActivityRecord) tmp.removeLast();
        } 
        
     //   System.out.println(System.currentTimeMillis() + " SMART " + id + " getUnitAnd returns " + a.identifier());
       
        if (tmp.size() == 0) { 
            unitAnd.remove(c);
        }
        
        size--;
        
        return a;
    }
    
    private ActivityRecord getOr(Context c, boolean head) { 
        
      //  System.out.println(System.currentTimeMillis() +" SMART " + id + " getOr " + c);
        
        if (c.isOr()) { 
            c = ((OrContext) c).getContexts()[0];
        }
              
        CircularBuffer tmp = or.get(c);

        if (tmp == null) { 
            return null;
        }

        ActivityRecord a;
        
        if (head) { 
            a = (ActivityRecord) tmp.removeFirst();
        } else { 
            a = (ActivityRecord) tmp.removeLast();
        }
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

     //   System.out.println(System.currentTimeMillis() + " SMART " + id + " getOr returns " + a.identifier() + " " + a.activity.getContext());
        
        size--;
        return a;
    }
    
    @Override
    public ActivityRecord dequeue(boolean head) {
    
        if (size == 0) { 
            return null;
        }
    
        if (unitAnd.size() > 0) {
            return getUnitAnd(unitAnd.keySet().iterator().next(), head);
        }
    
        if (or.size() > 0) { 
            return getOr(or.keySet().iterator().next(), head);
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
    
        System.err.println("EEP: ran into unknown Context Type ! " + c);
    }

    @Override
    public ActivityRecord steal(Context c, boolean head) {
       
        if (c.isUnit() || c.isAnd()) { 
        
            ActivityRecord a = getUnitAnd(c, head);
            
            if (a == null) { 
                a = getOr(c, head);
            }
            
            return a;
        }
        
        if (c.isOr()) { 
            
            Context [] and = ((OrContext) c).andContexts();
            
            if (and != null && and.length > 0) { 
                for (int i=0;i<and.length;i++) {
                    ActivityRecord a = getUnitAnd(and[i], head);
                    
                    if (a != null) { 
                        return a;
                    } 
 
                    a = getOr(c, head);
                    
                    if (a != null) { 
                        return a;
                    } 
                }
            } 
            
            Context [] unit = ((OrContext) c).unitContexts();
            
            if (unit != null && unit.length > 0) { 
                for (int i=0;i<unit.length;i++) {
                    ActivityRecord a = getUnitAnd(unit[i], head);
                    
                    if (a != null) { 
                        return a;
                    } 
 
                    a = getOr(c, head);
                    
                    if (a != null) { 
                        return a;
                    } 
                }
            } 
        }
   
        return null;
    }
}
