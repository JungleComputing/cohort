package ibis.cohort;

import java.io.Serializable;

public abstract class Context implements Serializable {
    
    /* Valid contexts: 
     * 
     *      context = UNIT | and | or
     *      and     = (UNIT & UNIT+)        
     *      or      = ((UNIT|AND) || (UNIT|AND)+)  
     */

    protected Context() { 
        // empty
    }
        
    public abstract boolean equals(Object other);
    public abstract boolean satisfiedBy(Context other);
        
    public boolean isUnit() { 
        return false;
    }
    
    public boolean isAnd() { 
        return false;
    }
    
    public boolean isOr() { 
        return false;
    }
}
