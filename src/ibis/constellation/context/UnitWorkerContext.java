package ibis.constellation.context;

import ibis.constellation.WorkerContext;

public class UnitWorkerContext extends WorkerContext {

    private static final long serialVersionUID = 6134114690113562356L;
    
    public static final long DEFAULT_RANK = 0;

    public static final byte BIGGEST  = 1;
    public static final byte SMALLEST = 2;
    public static final byte VALUE    = 3;
    public static final byte RANGE    = 4;
    public static final byte ANY      = 5;
    
    public static final UnitWorkerContext DEFAULT = new UnitWorkerContext("DEFAULT", ANY);
       
    public final String name;
    public final byte opcode; 
    public final long start; 
    public final long end;
    
    protected final int hashCode;

    public UnitWorkerContext(String name) {
    	this(name, ANY);
    }
    
    public UnitWorkerContext(String name, byte opcode) {
    	
    	super();
        
        if (name == null) { 
            throw new IllegalArgumentException("Context name cannot be null!");
        }

        switch (opcode) { 
        case BIGGEST:
        case SMALLEST:
        case ANY:
        	break;
        	
        case VALUE: 
        	throw new IllegalArgumentException("No value provided!");
            	
        case RANGE:
        	throw new IllegalArgumentException("No range provided!");
        	
        default:
        	throw new IllegalArgumentException("Unknown opcode!");
    	}
    	
    	this.name = name;
    	this.opcode = opcode; 
    	this.start = 0;
    	this.end = 0;

    	this.hashCode = name.hashCode();
    }

    public UnitWorkerContext(String name, byte opcode, long value) {

    	super();
        
        if (name == null) { 
            throw new IllegalArgumentException("Context name cannot be null!");
        }
    	
        switch (opcode) { 
        case BIGGEST:
        case SMALLEST:
        case ANY:
        	throw new IllegalArgumentException("No value allowed!");
        	
        case VALUE: 
        	break;
        	
        case RANGE:
        	throw new IllegalArgumentException("No range provided!");
        	
        default:
        	throw new IllegalArgumentException("Unknown opcode!");
    	}
    	
    	this.name = name;
    	this.opcode = opcode; 
    	this.start = value;
    	this.end = value;

    	this.hashCode = name.hashCode();
    }

    public UnitWorkerContext(String name, byte opcode, long start, long end) {

    	super();
        
        if (name == null) { 
            throw new IllegalArgumentException("Context name cannot be null!");
        }

        switch (opcode) { 
        case BIGGEST:
        case SMALLEST:
        case ANY:
        	throw new IllegalArgumentException("No range allowed!");
        	
        case VALUE: 
    		throw new IllegalArgumentException("No range allowed");
    	
        case RANGE:
        	break;
        		
        default:
        	throw new IllegalArgumentException("Unknown opcode!");
    	}
    	
    	this.name = name;
    	this.opcode = opcode; 
    	this.start = start;
    	this.end = end;

    	this.hashCode = name.hashCode();
    }
    
    @Override
    public boolean isUnit() { 
        return true;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    /*
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
    */
    
    public String toString() {
    	return "UnitWorkerContext(" + uniqueTag() + ")";
    }
    
    public String uniqueTag() { 
        String tmp = name + ", ";
        
        switch (opcode) { 
        case BIGGEST:
        	return tmp + "BIGGEST";
        case SMALLEST:
        	return tmp + "SMALLEST";
        case ANY:
        	return tmp + "ANY";
        case VALUE: 
        	return tmp + "VALUE(" + start + ")";
        case RANGE:
        	return tmp + "RANGE(" + start + " to " + end + ")";
    	default:
    		// should never happen!
    	    return tmp + "UNKNOWN";
        }
    }

    /*
    @Override
    public boolean satisfiedBy(WorkerContext offer) {
     
    	// This does NOT take the rank into account.     	
        if (offer == null) { 
            return false;
        }
        
        if (offer.isUnit()) { 
            
        	
        	
        	return equals(other);
        }
        
        if (offer.isOr()) { 
        	
        	
            return ((OrWorkerContext)other).contains(this);
        }
        
        return false;
    }
    */
    /*
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
    */
    
}
