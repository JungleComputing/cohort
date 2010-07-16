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
    public boolean equals(Object other) {
        
        if (!(other instanceof UnitContext)) { 
            return false;
        }
        
        return name.equals(((UnitContext)other).name);        
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
