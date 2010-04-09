package ibis.cohort.impl.distributed;

import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Context;
import ibis.cohort.context.AndContext;
import ibis.cohort.context.ContextSet;

import java.util.HashMap;
import java.util.Iterator;

public class ActivityRecordQueue {
    
    private final HashMap<ActivityIdentifier, ActivityRecord> map = 
        new HashMap<ActivityIdentifier, ActivityRecord>();
    
    private final HashMap<Context, HashMap<ActivityIdentifier, ActivityRecord>> 
        contextMap = new HashMap<Context, HashMap<ActivityIdentifier, ActivityRecord>>();
    
    public ActivityRecordQueue() { 
    }
    
    public synchronized boolean contains(ActivityIdentifier id) { 
        return map.containsKey(id);
    }
 
    private Context [] flatten(Context c) { 
        
        if (c == null) { 
            return new Context[0];
        } else if (c.isSet()) { 
            return ((ContextSet) c).getContexts();
        } else { 
            return new Context [] { c };
        }
     }
    
    
    public synchronized ActivityRecord remove(ActivityIdentifier id) { 
        
        ActivityRecord tmp = map.remove(id);
        
        if (tmp != null) { 
            
            Context [] c = flatten(tmp.activity.getContext());
            
            for (Context t : c) { 
                HashMap<ActivityIdentifier, ActivityRecord> m = contextMap.get(t);
                m.remove(id);
            }
        }
        
        return tmp;
    }
    
    public void add(ActivityRecord [] a) { 
    
        if (a == null || a.length == 0) { 
            return;
        }
    
        for (int i=0;i<a.length;i++) { 
            if (a[i] != null) { 
                add(a[i]);
            }
        }
    }
    
    public synchronized void add(ActivityRecord a) { 
        
        if (a == null) { 
            return;
        }
        
        map.put(a.identifier(), a);
   
        Context [] c = flatten(a.activity.getContext());
        
        for (Context t : c) { 
            HashMap<ActivityIdentifier, ActivityRecord> m = contextMap.get(t);
               
            if (m == null) { 
                m = new HashMap<ActivityIdentifier, ActivityRecord>();
                contextMap.put(t, m);
            } 
                    
            m.put(a.identifier(), a);
        }
    }
    
    public synchronized ActivityRecord get(ActivityIdentifier id) { 
        return map.get(id);
    }
    
    private ActivityIdentifier selectForSteal(Context [] as) { 
        
        for (Context a : as) { 
            
            HashMap<ActivityIdentifier, ActivityRecord> m = contextMap.get(a);
    
            if (m != null && m.size() > 0) { 
                // Man this is expensive!
                Iterator<ActivityIdentifier> itt = m.keySet().iterator();
                return itt.next();
            }
        }
        
        return null;
    } 
        
    private ActivityIdentifier sortAndSelectForSteal(AndContext [] as) { 
        
        if (as.length > 1) { 
            // TODO: sort such that the longest and is tested first.
        }
        
        return selectForSteal(as);
    }  
    
    public synchronized ActivityRecord steal(Context c) { 
       
        ActivityIdentifier id = null;
        
        if (c.isSet()) { 
            
            ContextSet s = (ContextSet) c;
           
            if (s.countAndContexts() > 0) { 
                id = sortAndSelectForSteal(s.andContexts());
            }  
         
            if (id == null && s.countUnitContexts() > 0) { 
                id = selectForSteal(s.unitContexts());
            }
       
        } else { 
            id = selectForSteal(new Context[] { c });
        } 
    
        if (id == null) { 
            // No suitable ActivityRecord found!
            return null;
        } 
        
        return remove(id);
    }
    
}
