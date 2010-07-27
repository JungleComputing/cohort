package ibis.cohort;

import java.io.Serializable;

public abstract class Context implements Serializable {
    
    /* Valid contexts: 
     * 
     *      context = UNIT | and | or
     *      and     = (UNIT & UNIT+)        
     *      or      = ((UNIT|AND) || (UNIT|AND)+)  
     */

    // Is this context resticted to the local machine ? 
    public final boolean restrictedToLocal;
    
    protected Context(boolean restrictedToLocal) { 
        this.restrictedToLocal = restrictedToLocal;
    }
        
    public abstract boolean equals(Object other);
    public abstract boolean satisfiedBy(Context other);
    
    public boolean isRestrictedToLocal() { 
        return restrictedToLocal;
    }    
    
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
