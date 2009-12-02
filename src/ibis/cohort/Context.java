package ibis.cohort;

import ibis.cohort.context.AnyContext;
import ibis.cohort.context.CohortContext;
import ibis.cohort.context.LocalContext;

import java.io.Serializable;

public abstract class Context implements Serializable {
    
    /* Valid contexts: 
     * 
     *      context = LOCAL | ANY | COHORT | UNIT | and
     *      and     = (UNIT UNIT+) | (COHORT UNIT+)       
     *      
     * Valid contextsets:
     * 
     *      set     = ANY | LOCAL | COHORT | 
     *                (UNIT (UNIT* | and*)) | (and (UNIT* | and*))
     *      
     */
    
    // Special values     
    public final static Context LOCAL  = new LocalContext();
    public final static Context COHORT = new CohortContext();    
    public final static Context ANY    = new AnyContext();

    protected Context() { 
        // empty
    }
        
    public abstract boolean equals(Object other);
    public abstract boolean contains(Context other);
    
    public boolean isLocal() { 
        return false;
    }    
    
    public boolean isCohort() { 
        return false;
    }
    
    public boolean isAny() { 
        return false;
    }
    
    public boolean isUnit() { 
        return false;
    }
    
    public boolean isAnd() { 
        return false;
    }
    
    public boolean isSet() { 
        return false;
    }

}
