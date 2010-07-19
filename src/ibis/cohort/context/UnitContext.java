package ibis.cohort.context;

import ibis.cohort.Context;

public class UnitContext extends Context {

    private static final long serialVersionUID = 6134114690113562356L;
    
    public final String name; 
    
    public UnitContext(String name) {         
        this.name = name;
    }
    
    @Override
    public boolean isUnit() { 
        return true;
    }
    
    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final UnitContext other = (UnitContext) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        return true;
    }

    @Override
    public boolean contains(Context other) {
        return equals(other);
    } 
    
    public String toString() { 
        return "UnitContext(" + name + ")";
    }

    @Override
    public boolean satisfiedBy(Context other) {
     
        // TODO: not sure if this is complete!
        
        if (other.isAnd()) { 
            return false;
        }
       
        if (other.isUnit()) { 
            return equals(other);
        }
        
        if (other.isSet()) { 
            return ((ContextSet)other).contains(this);
        }
        
        return false;
    }
}
