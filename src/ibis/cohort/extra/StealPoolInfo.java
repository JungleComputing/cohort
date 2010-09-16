package ibis.cohort.extra;

import ibis.cohort.StealPool;
import ibis.ipl.IbisIdentifier;

import java.util.ArrayList;
import java.util.HashMap;

public class StealPoolInfo {

    private final HashMap<String, ArrayList> map =
        new HashMap<String, ArrayList>();
    
    public StealPoolInfo() { 
        // Nothing to see here... move along!
    }
    
    private void remove(String tag, Object o) {
     
        ArrayList tmp = map.get(tag);
        
        if (tmp == null || !tmp.contains(o)) { 
            System.err.println("EEP: failed remove from StealPoolInfo!");
            return;
        }

        tmp.remove(o);
    }
    
    private void add(String tag, Object o) {
        
        ArrayList tmp = map.get(tag);
        
        if (tmp == null) { 
            tmp = new ArrayList();
        }

        // Sanity check, should never happen ? 
        if (tmp.contains(o)) { 
            System.err.println("EEP: double add to StealPoolInfo");
            return;
        }
        
        tmp.add(o);
    }
    
    private void remove(StealPool pool, Object o) {
    
        if (pool.isSet()) { 
            StealPool [] tmp = pool.set();
            
            for (int i=0;i<tmp.length;i++) { 
                remove(tmp[i].getTag(), o);
            }
        } else { 
            remove(pool.getTag(), o);
        }
    }
        
    private void add(StealPool pool, Object o) {

        if (pool.isSet()) { 
            StealPool [] tmp = pool.set();
            
            for (int i=0;i<tmp.length;i++) { 
                add(tmp[i].getTag(), o);
            }
        } else { 
            add(pool.getTag(), o);
        }
    }
    
    public synchronized void update(StealPool oldPool, StealPool newPool, Object o) { 
        
        if (oldPool != null) { 
            remove(oldPool, o); 
        }
        
        if (newPool != null) { 
            add(oldPool, o);
        }
    }

    public IbisIdentifier selectRandom(String tag) {
        return null;
    }
}
