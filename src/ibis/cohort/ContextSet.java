package ibis.cohort;


import ibis.cohort.context.AndContext;
import ibis.cohort.context.UnitContext;

import java.util.ArrayList;

public class ContextSet extends Context {

    private static final long serialVersionUID = -1202476921345669674L;

    private final ArrayList<AndContext> andContexts = 
        new ArrayList<AndContext>();
    
    private final ArrayList<UnitContext> unitContexts = 
        new ArrayList<UnitContext>(); 

    public ContextSet(UnitContext u) {        
        unitContexts.add(u);
    }

    public ContextSet(AndContext a) {
        andContexts.add(a);
    }

    public void add(UnitContext u) {
        unitContexts.add(u);
    }

    public void add(AndContext a) { 
        andContexts.add(a);
    }
    
    public void add(ContextSet s) {
        
        for (UnitContext u : s.unitContexts) { 
            unitContexts.add(u);
        }

        for (AndContext a : s.andContexts) { 
            andContexts.add(a);
        }
    }

    public boolean contains(UnitContext u) {

        if (unitContexts.contains(u)) { 
            return true;
        }

        for (AndContext tmp2 : andContexts) {

            if (tmp2.contains(u)) { 
                return true;
            }
        }

        return false;
    }    

    public boolean contains(AndContext u) {

        for (AndContext tmp : andContexts) {

            if (tmp.contains(u)) { 
                return true;
            }
        }

        return false;
    }

    public boolean contains(ContextSet other) {

        for (UnitContext u : other.unitContexts) { 

            if (!contains(u)) { 
                return false;
            }            
        }

        for (AndContext a : other.andContexts) {

            if (!contains(a)) { 
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean isSet() { 
        return true;
    }

    public boolean isCompound() { 
        return (unitContexts != null);
    }

    @Override
    public boolean contains(Context other) {

        if (other.isUnit()) { 
            return contains((UnitContext)other);
        }

        if (other.isAnd()) { 
            return contains((AndContext)other);
        }

        if (other.isSet()) { 
            return contains((ContextSet)other);
        }

        return false;
    }
    
    @Override
    public boolean equals(Object other) {
        
        if (!(other instanceof ContextSet)) { 
            return false;
        }

        ContextSet o = (ContextSet) other;

        if (unitContexts.size() != o.unitContexts.size()) { 
            return false;
        }

        if (andContexts.size() != o.andContexts.size()) { 
            return false;
        }

        return unitContexts.containsAll(o.unitContexts) && 
            o.unitContexts.containsAll(unitContexts) &&
            andContexts.containsAll(o.andContexts) && 
            o.andContexts.containsAll(andContexts);
    }    
}
