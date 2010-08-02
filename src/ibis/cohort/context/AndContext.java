package ibis.cohort.context;

import java.util.Arrays;
import java.util.Comparator;

import ibis.cohort.Context;
import ibis.cohort.context.UnitContext.UnitContextSorter;

public class AndContext extends Context {
    
    private static final long serialVersionUID = -8084449299649996481L;
    
    protected final UnitContext [] unitContexts;
    protected final int hashCode;
    
    public AndContext(UnitContext [] and) { 
      
        super();
        
        if (and == null || and.length < 2) { 
            throw new IllegalArgumentException("Illegal argument while " +
                        "creating AndContext: " + and);
        }
        
        unitContexts = UnitContext.sort(and);
        hashCode = UnitContext.generateHash(and);
    }
    
    @Override
    public boolean isAnd() { 
        return true;
    }
        
    public String toString() { 
        StringBuilder b = new StringBuilder();
        
        for (int i=0;i<unitContexts.length;i++) { 
            b.append(unitContexts[i]);
           
            if (i<unitContexts.length-1) { 
                b.append(" and ");
            }
        }
        
        return b.toString();
    }

    @Override
    public boolean satisfiedBy(Context other) {
 
        if (other == null) { 
            return false;
        }
        
        if (other.isAnd()) { 
            return equals(other);
        }
        
        return false;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        
        if (this == obj) {
            return true;
        }
            
        if (obj == null) {
            return false;
        }
        
        if (getClass() != obj.getClass()) {
            return false;
        }
        
        AndContext other = (AndContext) obj;
        
        if (hashCode != other.hashCode) { 
            return false;
        }
        
        if (unitContexts.length != other.unitContexts.length) { 
            return false;
        }
        
        for (int i=0;i<unitContexts.length;i++) { 
            if (!unitContexts[i].equals(other.unitContexts[i])) {
                return false;
            }
        }
        
        return true;
    }
    
    protected static class AndContextSorter implements Comparator<AndContext> {

        public int compare(AndContext u1, AndContext u2) {
            
            if (u1.hashCode == u2.hashCode) { 
                return 0;
            } else if (u1.hashCode < u2.hashCode) { 
                return -1;
            } else {    
                return 1;
            }
        }
    }
    
    public static AndContext [] sort(AndContext [] in) { 
        Arrays.sort(in, new AndContextSorter());
        return in;
    }
    
    public static int generateHash(AndContext [] in) { 
      
        // NOTE: result depends on order of elements in array! 
        int hashCode = 1;
       
        for (int i=0;i<in.length;i++) {
            hashCode = 31*hashCode + (in[i] == null ? 0 : in[i].hashCode);
        }
       
        return hashCode;
    }
}
