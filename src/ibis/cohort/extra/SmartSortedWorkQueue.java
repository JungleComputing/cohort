package ibis.cohort.extra;

import ibis.cohort.ActivityContext;
import ibis.cohort.WorkerContext;
import ibis.cohort.context.OrActivityContext;
import ibis.cohort.context.UnitActivityContext;
import ibis.cohort.context.UnitWorkerContext;
import ibis.cohort.context.OrWorkerContext;
import ibis.cohort.impl.distributed.ActivityRecord;

import java.util.HashMap;

public class SmartSortedWorkQueue extends WorkQueue {

    // We maintain two lists here, which reflect the relative complexity of 
    // the context associated with the jobs: 
    //
    // 'UNIT' jobs are likely to have limited suitable locations, but 
    //     their context matching is easy
    // 'OR' jobs have more suitable locations, but their context matching may be 
    //     more expensive

    protected final HashMap<String, SortedList> unit = 
        new HashMap<String, SortedList>();

    protected final HashMap<String, SortedList> or = 
        new HashMap<String, SortedList>();

    protected int size;
    
    public SmartSortedWorkQueue(String id) { 
        super(id);
    }

    @Override
    public int size() {
        return size;
    }

    private ActivityRecord getUnit(String name, boolean head) { 

    	SortedList tmp = unit.get(name);

        if (tmp == null) { 
            return null;
        }
        
        ActivityRecord a;
        
        if (head) {
        	a = (ActivityRecord) tmp.removeHead();
        } else { 
        	a = (ActivityRecord) tmp.removeTail();
        }
    	
        if (tmp.size() == 0) { 
            unit.remove(name);
        }
        
        size--;
        
        return a;
    }
    	
    
    private ActivityRecord getUnit(UnitWorkerContext c) { 

        SortedList tmp = unit.get(c.name);

        if (tmp == null) { 
        //	System.out.println(id + "   GetUnit " + c.name + " empty! " + unit.size() + " " + unit.keySet());
            return null;
        }

        ActivityRecord a = null;
        
        switch (c.opcode) { 
        case UnitWorkerContext.BIGGEST:
        case UnitWorkerContext.ANY:
        	a = (ActivityRecord) tmp.removeTail();
        	break;
        	
        case UnitWorkerContext.SMALLEST:
        	a = (ActivityRecord) tmp.removeHead();
        	break;
        	
        case UnitWorkerContext.VALUE:
        case UnitWorkerContext.RANGE:
        	a = tmp.removeOneInRange(c.start, c.end);
        	break;
        }
        
       // System.out.println(id + "   GetUnit " + c.name + " succeeded!");
        
        if (tmp.size() == 0) { 
            unit.remove(c.name);
        }

        size--;

        return a;
    }
    
    private ActivityRecord getOr(String name, boolean head) { 

    	SortedList tmp = or.get(name);

    	if (tmp == null) {
    		return null;
    	}

    	ActivityRecord a = null;
    	
    	if (head) {
        	a = (ActivityRecord) tmp.removeHead();
        } else { 
        	a = (ActivityRecord) tmp.removeTail();
        }
    	
    	if (tmp.size() == 0) { 
            or.remove(name);
        }

        // Remove entry for this ActivityRecord from all lists.... 
        UnitActivityContext[] all = ((OrActivityContext) a.activity.getContext()).getContexts();

        for (int i=0;i<all.length;i++) { 

            // Remove this activity from all entries in the 'or' table
            tmp = or.get(all[i].name);

            if (tmp != null) { 
                tmp.removeByReference(a);

                if (tmp.size()== 0) { 
                    or.remove(all[i].name);
                }
            }
        }

        size--;
        return a;
    } 
    	
    private ActivityRecord getOr(UnitWorkerContext c) { 

        SortedList tmp = or.get(c.name);

        if (tmp == null) {
        //	System.out.println(id + "   GetOR empty!");            		
            return null;
        }

    //	System.out.println(id + "   GetOR NOT empty!");            		
        
        ActivityRecord a = null;
        
        switch (c.opcode) { 
        case UnitWorkerContext.BIGGEST:
        case UnitWorkerContext.ANY:
        	a = (ActivityRecord) tmp.removeTail();
        	break;
        	
        case UnitWorkerContext.SMALLEST:
        	a = (ActivityRecord) tmp.removeHead();
        	break;
        	
        case UnitWorkerContext.VALUE:
        case UnitWorkerContext.RANGE:
        	a = tmp.removeOneInRange(c.start, c.end);
        	break;
        }

        if (tmp.size() == 0) { 
            or.remove(c.name);
        }
        
        // Remove entry for this ActivityRecord from all lists.... 
        OrActivityContext cntx = (OrActivityContext) a.activity.getContext();
        
        for (int i=0;i<cntx.size();i++) { 

        	UnitActivityContext u = cntx.get(i);
        	
            // Remove this activity from all entries in the 'or' table
            tmp = or.get(u.name);

            if (tmp != null) { 
                tmp.removeByReference(a);

                if (tmp.size()== 0) { 
                    or.remove(u.name);
                }
            }
        }

        size--;
        return a;
    }

    @Override
    public ActivityRecord dequeue(boolean head) {

        if (size == 0) { 
            return null;
        }

        if (unit.size() > 0) {
            return getUnit(unit.keySet().iterator().next(), head);
        }

        if (or.size() > 0) { 
            return getOr(or.keySet().iterator().next(), head);
        } 


        return null;
    }

    private void enqueueUnit(UnitActivityContext c, ActivityRecord a) {

    	//System.out.println(id + "    ENQUEUE UNIT: " + c);
    	
        SortedList tmp = unit.get(c.name);

        if (tmp == null) { 
            tmp = new SortedList(c.name);
            unit.put(c.name, tmp);
        }

        tmp.insert(a, c.rank);
        size++;
    }

    private void enqueueOr(OrActivityContext c, ActivityRecord a) {

    	//System.out.println(id + "    ENQUEUE OR: " + c);
    	
        for (int i=0;i<c.size();i++) { 
            
        	UnitActivityContext uc = c.get(i);
        
        	SortedList tmp = or.get(uc.name);

            if (tmp == null) { 
                tmp = new SortedList(uc.name);
                or.put(uc.name, tmp);
            }

            tmp.insert(a, uc.rank);
            
       //     System.out.println(id + "    ENQUEUE " + uc.name);
        }

        size++;
    }


    @Override
    public void enqueue(ActivityRecord a) {

    	ActivityContext c = a.activity.getContext();

    //	System.out.println(id + "   1 ENQUEUE " + c);
    	
        if (c.isUnit()) {
            enqueueUnit((UnitActivityContext) c, a);
            return;
        }

        if (c.isOr()) {
            enqueueOr((OrActivityContext) c, a);
            return;
        }

        System.out.println(id + "EEP: ran into unknown Context Type ! " + c);
    }
    
    @Override
    public ActivityRecord steal(WorkerContext c) {

    	//System.out.println(id + "   STEAL: " + c);
    	
    	if (c.isUnit()) { 

        	UnitWorkerContext tmp = (UnitWorkerContext) c;
        	
            ActivityRecord a = getUnit(tmp);

            if (a == null) { 
                a = getOr(tmp);
            }

            return a;
        }

        if (c.isOr()) { 

        //	System.out.println(id + "  STEAL is OR");
        	
        	OrWorkerContext o = (OrWorkerContext) c;
        	
            for (int i=0;i<o.size();i++) {
            	
            	UnitWorkerContext ctx = o.get(i);

          //  	System.out.println(id + "   STEAL attempt from unit with " + ctx);            		
            	
            	ActivityRecord a = getUnit(ctx); 

            	if (a != null) { 
            		return a;
            	} 

           // 	System.out.println(id + "   STEAL attempt from or with " + ctx);            		
            	
            	a = getOr(ctx);

            	if (a != null) { 
            		return a;
            	} 
            } 
        }

        return null;
    }
}




