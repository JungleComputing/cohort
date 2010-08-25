package ibis.cohort;

import java.io.Serializable;

public class WorkerContext implements Serializable {
    
	protected WorkerContext() { 
        // empty
    
	}
        
    //public abstract boolean equals(Object other);
    //public abstract boolean satisfiedBy(WorkerContext other);
    
    public boolean isUnit() { 
        return false;
    }
    
    public boolean isOr() { 
        return false;
    }

	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}    
}
