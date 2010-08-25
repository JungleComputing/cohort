package ibis.cohort.context;

import java.util.Arrays;
import java.util.Comparator;

import ibis.cohort.ActivityContext;
import ibis.cohort.WorkerContext;

public class UnitActivityContext extends ActivityContext {

    private static final long serialVersionUID = 6134114690113562356L;

    public static final long DEFAULT_RANK = 0;
    
    public static final UnitActivityContext DEFAULT = new UnitActivityContext("DEFAULT", DEFAULT_RANK);
       
    public final String name; 
    public final long rank; 
    
    protected final int hashCode;
    
    public UnitActivityContext(String name, long rank) {
        
        super();
        
        if (name == null) { 
            throw new IllegalArgumentException("Context name cannot be null!");
        }
        
        this.name = name;
        this.rank = rank;
        this.hashCode = name.hashCode();
    }
    
    public UnitActivityContext(String name) {
    	this(name, DEFAULT_RANK);
    }
    
    @Override
    public boolean isUnit() { 
        return true;
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
      
        UnitActivityContext other = (UnitActivityContext) obj;
        
        if (hashCode != other.hashCode) { 
            return false;
        }
        
        return (rank == other.rank && name.equals(other.name));
    }
    
    public String toString() { 
        return "UnitActivityContext(" + name + ", " + rank + ")";
    }

    public boolean satisfiedBy(UnitWorkerContext offer) {

    	if (!name.equals(offer.name)) { 
    		return false;
    	}
    	
    	switch (offer.opcode) { 
    	case UnitWorkerContext.BIGGEST: 
    	case UnitWorkerContext.SMALLEST:
    	case UnitWorkerContext.ANY:
    		return true;

    	case UnitWorkerContext.VALUE:
    		return (rank == offer.start);
    		
    	case UnitWorkerContext.RANGE:
    		return (rank >= offer.start && rank <= offer.end); 
    	}
    	
    	return false;
    } 
    
    public boolean satisfiedBy(OrWorkerContext offer) {
    	
    	for (int i=0;i<offer.size();i++) { 
    		
    		UnitWorkerContext c = offer.get(i);

    		if (satisfiedBy(c)) { 
    			return true;
    		}
    	
    	}
    	
    	return false;
    }
    
    @Override
    public boolean satisfiedBy(WorkerContext offer) {
     
    	// This does NOT take the rank into account.     	
        if (offer == null) { 
            return false;
        }
        
        if (offer.isUnit()) { 
        	return satisfiedBy((UnitWorkerContext)offer);
        }
        
        if (offer.isOr()) { 
        	return satisfiedBy((OrWorkerContext)offer);
        }
        
        return false;
    }
    
    protected static class UnitActivityContextSorter implements Comparator<UnitActivityContext> {

        public int compare(UnitActivityContext u1, UnitActivityContext u2) {
            
            if (u1.hashCode == u2.hashCode) { 
            	
            	if (u1.rank == u2.rank) { 
            		return 0;
            	} else if (u1.rank < u2.rank) { 
            		return -1;
            	} else { 
            		return 1;
            	}
            	
            } else if (u1.hashCode < u2.hashCode) { 
                return -1;
            } else {    
                return 1;
            }
        }
    }
    
    public static UnitActivityContext [] sort(UnitActivityContext [] in) { 
        Arrays.sort(in, new UnitActivityContextSorter());
        return in;
    }
    
    public static int generateHash(UnitActivityContext [] in) { 
      
        // NOTE: result depends on order of elements in array!
    	// NOTE: does not take rank into account
    	
        int hashCode = 1;
       
        for (int i=0;i<in.length;i++) {
            hashCode = 31*hashCode + (in[i] == null ? 0 : in[i].hashCode);
        }
       
        return hashCode;
    }
}
