package ibis.cohort.impl.distributed;

import java.util.HashMap;

import ibis.cohort.ActivityIdentifier;
import ibis.cohort.CohortIdentifier;

public class LocationCache {
    
    private HashMap<ActivityIdentifier, Entry> map = 
        new HashMap<ActivityIdentifier, Entry>();
    
    public final class Entry {
        
        public final CohortIdentifier id; 
        public final long count;
        
        Entry(CohortIdentifier id, long count) { 
            this.id = id; 
            this.count = count;
        }
    }
    
    public synchronized CohortIdentifier lookup(ActivityIdentifier a) {
        
        final Entry tmp = map.get(a);
        
        if (tmp != null) { 
            return tmp.id;
        } else { 
            return null;
        }
    }
    
    public synchronized Entry lookupEntry(ActivityIdentifier a) {
        return map.get(a);
    }
    
    public synchronized CohortIdentifier remove(ActivityIdentifier a) {
        
        final Entry tmp = map.remove(a);
        
        if (tmp != null) { 
            return tmp.id;
        } else { 
            return null;
        }
    }
    
    public synchronized void removeIfEqual(ActivityIdentifier a, 
            CohortIdentifier c) { 

        final Entry tmp = map.get(a);
        
        if (tmp == null) { 
            return;
        }
        
        if (tmp.id.equals(c)) { 
            map.remove(a);
        }
    }
    
    public synchronized void put(ActivityIdentifier a, 
            CohortIdentifier c, long count) {

        // NOTE: we only replace an existing entry if count is larger 
        final Entry tmp = map.get(a);
        
        if (tmp == null) { 
            map.put(a, new Entry(c, count));
        } else if (tmp.count < count) { 
            map.put(a, new Entry(c, count));
        } else if (tmp.count == count && !tmp.id.equals(c)) { 
            // SANITY CHECK
            System.out.println("ERROR:  Inconsistency discovered in " 
                        + "LocactionCache: " + tmp.id + "/" + tmp.count 
                        + " != " + c + "/" + count);
        }
    }
}
