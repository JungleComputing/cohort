package ibis.cohort.extra;

import ibis.cohort.Context;
import ibis.cohort.context.OrContext;
import ibis.cohort.context.UnitContext;
import ibis.cohort.impl.distributed.ActivityRecord;

import java.util.Comparator;
import java.util.HashMap;

public class SmartSortedWorkQueue extends WorkQueue {

    // We maintain two lists here, which reflect the relative complexity of 
    // the context associated with the jobs: 
    //
    // 'UNIT or AND' jobs are likely to have limited suitable locations, but 
    //     their context matching is easy
    // 'OR' jobs have more suitable locations, but their context matching may be 
    //     expensive

    protected final HashMap<Context, SortedList> unitAnd = 
        new HashMap<Context, SortedList>();

    protected final HashMap<Context, SortedList> or = 
        new HashMap<Context, SortedList>();

    protected int size;

    private final Comparator comparator;
    
    public SmartSortedWorkQueue(String id, Comparator comparator) { 
        super(id);
        this.comparator = comparator;
    }

    @Override
    public int size() {
        return size;
    }

    private ActivityRecord getUnitAnd(Context c, boolean head) { 

        //   System.out.println(System.currentTimeMillis() +" SMART " + id + " getUnitAnd " + c);

        SortedList tmp = unitAnd.get(c);

        if (tmp == null) { 
            // System.out.println(System.currentTimeMillis() + " SMART " + id + " getUnitAnd empty!");
            return null;
        }

        ActivityRecord a;
        
        if (head) { 
            a = (ActivityRecord) tmp.removeHead();
        } else { 
            a = (ActivityRecord) tmp.removeTail();
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

        SortedList tmp = or.get(c);

        if (tmp == null) { 
            return null;
        }

        ActivityRecord a;
        
        if (head) {
            a = (ActivityRecord) tmp.removeHead();
        } else { 
            a = (ActivityRecord) tmp.removeTail();
        }

        Context [] all = ((OrContext) a.activity.getContext()).getContexts();

        for (int i=0;i<all.length;i++) { 

            // Remove this activity from all entries in the 'or' table
            tmp = or.get(all[i]);

            if (tmp != null) { 
                tmp.removeByReference(a);

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

        SortedList tmp = unitAnd.get(c);

        if (tmp == null) { 
            tmp = new SortedList(comparator);
            unitAnd.put(c, tmp);
        }

        tmp.insert(a);
        size++;
    }

    // NOTE: only works for Unit and and contexts
    private void enqueueOr(OrContext c, ActivityRecord a) {

        Context [] all = ((OrContext) c).getContexts();

        for (int i=0;i<all.length;i++) { 

            SortedList tmp = or.get(all[i]);

            if (tmp == null) { 
                tmp = new SortedList(comparator);
                or.put(all[i], tmp);
            }

            tmp.insert(a);
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

    	System.out.println("STEAL " + c + " " + head);
    	
        if (c.isUnit() || c.isAnd()) { 

        	System.out.println("STEAL unit or and");
        	        	
            ActivityRecord a = getUnitAnd(c, head);

            if (a == null) { 
                a = getOr(c, head);
            }

            return a;
        }

        if (c.isOr()) { 

        	System.out.println("STEAL or");
        	
        	Context [] and = ((OrContext) c).andContexts();

        	System.out.println("STEAL or: and = " + (and == null ? 0 : and.length));        	
        	
            if (and != null && and.length > 0) { 
                for (int i=0;i<and.length;i++) {
                    ActivityRecord a = getUnitAnd(and[i], head);

                    if (a != null) { 
                        return a;
                    } 

                    a = getOr(and[i], head);

                    if (a != null) { 
                        return a;
                    } 
                }
            } 

            Context [] unit = ((OrContext) c).unitContexts();

            System.out.println("STEAL or: or = " + (unit == null ? 0 : unit.length));        	
        	
            if (unit != null && unit.length > 0) { 
                for (int i=0;i<unit.length;i++) {
                	
                	System.out.println("STEAL or: or : " + unit[i]);        	
                	    	
                    ActivityRecord a = getUnitAnd(unit[i], head);

                    if (a != null) { 
                        return a;
                    } 

                    a = getOr(unit[i], head);

                    if (a != null) { 
                        return a;
                    } 
                }
            } 
        }

        return null;
    }
}




