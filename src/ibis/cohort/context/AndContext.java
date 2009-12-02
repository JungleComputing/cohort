package ibis.cohort.context;

import ibis.cohort.Context;

import java.util.ArrayList;

public class AndContext extends Context {
    
    private static final long serialVersionUID = -8084449299649996481L;
    
    public final ArrayList<UnitContext> contexts = new ArrayList<UnitContext>(); 
    
    public AndContext(UnitContext a, UnitContext b) { 
        contexts.add(a);
        contexts.add(b);
    }
    
    protected void add(UnitContext c) { 
        contexts.add(c);
    }

    @Override
    public boolean equals(Object other) {
        
        if (!(other instanceof AndContext)) {
            return false;
        }
        
        AndContext o = (AndContext) other;
            
        if (contexts.size() != o.contexts.size()) { 
            return false;
        }

        for (UnitContext tmp : contexts) {             
            if (!o.contains(tmp)) { 
                return false;
            }                 
        }

        return true;
    }
    
    public boolean contains(UnitContext c) { 
        return contexts.contains(c);
    }

    public boolean contains(AndContext c) { 
        
        // Check if all context in c are also in this context. 
        for (UnitContext tmp : c.contexts) {         
            if (!contains(tmp)) { 
                return false;
            }
        }
        
        return true;
    }

    @Override
    public boolean contains(Context other) {
        
        if (other.isUnit()) { 
            return contains((UnitContext)other);
        }

        if (other.isAnd()) { 
            return contains((AndContext)other);
        }
        
        return false;
    }
}
