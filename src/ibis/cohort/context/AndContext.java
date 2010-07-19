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
    public boolean equals(Object obj) {

        // NOTE: Very expensive operation!
        
        if (this == obj)
            return true;
        
        if (obj == null)
            return false;
        
        if (getClass() != obj.getClass())
            return false;
        
        final AndContext other = (AndContext) obj;
            
        if (contexts.size() != other.contexts.size()) { 
            return false;
        }

        for (UnitContext tmp : contexts) {             
            if (!other.contains(tmp)) { 
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
    
    public String toString() { 
        StringBuilder b = new StringBuilder();
        
        for (UnitContext u : contexts) { 
            b.append(u);
            b.append(" and ");
        }

        return b.toString();
    }

    @Override
    public boolean satisfiedBy(Context other) {
 
        // TODO: incomplete ?
        if (other.isUnit()) { 
            return false;
        }
        
        if (other.isAnd()) { 
            return equals(other);
        }
     
        return false;
    }

    @Override
    public int hashCode() {
        final int PRIME = 31;
        int result = 1;
        result = PRIME * result + ((contexts == null) ? 0 : contexts.hashCode());
        return result;
    }
}
