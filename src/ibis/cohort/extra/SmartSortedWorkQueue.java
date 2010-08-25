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
        
        return a;
    }
    	
    
    private ActivityRecord getUnit(UnitWorkerContext c) { 

        SortedList tmp = unit.get(c.name);

        if (tmp == null) { 
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

        if (tmp.size() == 0) { 
            or.remove(c.name);
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

        SortedList tmp = unit.get(c.name);

        if (tmp == null) { 
            tmp = new SortedList(c.name);
            unit.put(c.name, tmp);
        }

        tmp.insert(a, c.rank);
        size++;
    }

    private void enqueueOr(OrActivityContext c, ActivityRecord a) {

        UnitActivityContext [] all = c.getContexts();

        for (int i=0;i<all.length;i++) { 

            SortedList tmp = or.get(all[i].name);

            if (tmp == null) { 
                tmp = new SortedList(all[i].name);
                or.put(all[i].name, tmp);
            }

            tmp.insert(a, all[i].rank);
        }

        size++;
    }


    @Override
    public void enqueue(ActivityRecord a) {

        ActivityContext c = a.activity.getContext();

        if (c.isUnit()) {
            enqueueUnit((UnitActivityContext) c, a);
            return;
        }

        if (c.isOr()) {
            enqueueOr((OrActivityContext) c, a);
            return;
        }

        System.err.println("EEP: ran into unknown Context Type ! " + c);
    }
    
    @Override
    public ActivityRecord steal(WorkerContext c) {

    	if (c.isUnit()) { 

        	UnitWorkerContext tmp = (UnitWorkerContext) c;
        	
            ActivityRecord a = getUnit(tmp);

            if (a == null) { 
                a = getOr(tmp);
            }

            return a;
        }

        if (c.isOr()) { 
        	
            UnitWorkerContext [] unit = ((OrWorkerContext) c).getContexts();
        	
            for (int i=0;i<unit.length;i++) {                	
            	ActivityRecord a = getUnit(unit[i]); 

            	if (a != null) { 
            		return a;
            	} 

            	a = getOr(unit[i]);

            	if (a != null) { 
            		return a;
            	} 
            } 
        }

        return null;
    }
}




