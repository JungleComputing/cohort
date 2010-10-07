package ibis.constellation.context;

import ibis.constellation.ActivityContext;
import ibis.constellation.WorkerContext;

public class OrActivityContext extends ActivityContext {

    private static final long serialVersionUID = -1202476921345669674L;

    protected final UnitActivityContext [] unitContexts;
    
    protected final int hashCode;
    protected final boolean ordered;    
    
    public OrActivityContext(UnitActivityContext [] unit, boolean ordered) {        
        super();
      
        if (unit == null || unit.length < 2) { 
        	  throw new IllegalArgumentException("Invalid arguments to " +
                      "OrContext: 2 or more contexts required!");
        }
    
        unitContexts = unit;
        this.ordered = ordered;
        
        if (!ordered) { 
        	// When the OrContext is unordered, the order of the elements is unimportant. 
        	// We therefore sort it to get a uniform order, regardless of the user defined 
        	// order.
        	UnitActivityContext.sort(unitContexts);
        }

        hashCode = 31*UnitActivityContext.generateHash(unitContexts);     
    }

    public OrActivityContext(UnitActivityContext [] unit) {
    	this(unit, false);
    }

    public int size() { 
    	return unitContexts.length;
    }
    
    public UnitActivityContext get(int index) { 
    	return unitContexts[index];
    }
        
    public boolean contains(UnitActivityContext u) {
        
    	// TODO: use the fact that the unitContexts are sorted!!!!    	
        for (int i=0;i<unitContexts.length;i++) { 
            if (u.equals(unitContexts[i])) { 
                return true;
            }
        }
        
        return false;
    }    

    /*
    public boolean contains(OrActivityContext other) {

    	if (other == this) { 
    		return true;
    	}
    	
    	for (UnitActivityContext u : other.unitContexts) { 
    		if (!contains(u)) { 
    			return false;
            }
        }

        return true;
    }

    public boolean overlapping(OrActivityContext other) {

    	// TODO: use the fact that the unitContexts are sorted!!!!    	
        if (unitContexts.length != 0 && other.unitContexts.length != 0) {
            for (UnitActivityContext u : other.unitContexts) { 
                if (contains(u)) { 
                    return true;
                }            
            }
        }

        return false;
    }
    */
    
    public int countUnitContexts() { 
        return unitContexts.length;
    }
    
    public UnitActivityContext [] getContexts() { 
    	return unitContexts.clone();
    }
    
    @Override
    public boolean isOr() { 
        return true;
    }
    
    /*
    public boolean contains(ActivityContext other) {

        if (other.isUnit()) { 
            return contains((UnitActivityContext)other);
        }

        if (other.isOr()) { 
            return contains((OrActivityContext)other);
        }

        return false;
    }
    */
    
    @Override
    public int hashCode() { 
        return hashCode;
    }
        
    @Override
    public boolean equals(Object obj) {

        // NOTE: potentially very expensive operations, especially with large 
        //       contexts that are equal
       
        if (this == obj) { 
            return true;
        }
        
        if (obj == null) { 
            return false;
        }
        
        if (getClass() != obj.getClass()) { 
            return false;
        }
        
        OrActivityContext other = (OrActivityContext) obj;
        
        if (hashCode != other.hashCode) { 
            return false;
        }
        
        if (unitContexts.length != other.unitContexts.length) { 
            return false;
        }

        for (int i=0;i<unitContexts.length;i++) { 
            if (!other.contains(unitContexts[i])) { 
                return false;
            }
        }
     
        return true;
    }    
    
    public String toString() { 
        
        StringBuilder b = new StringBuilder();
        
        b.append("OrContext(");
        
        for (int i=0;i<unitContexts.length;i++) { 
            b.append(unitContexts[i]);
            
            if (i != unitContexts.length-1) { 
                b.append(" or ");
            }
        }
        
         b.append(")");
        
        return b.toString();
    }

    private boolean satisfiedBy(UnitWorkerContext offer) {
    	
    	for (int i=0;i<unitContexts.length;i++) { 
    		
    		UnitActivityContext tmp = unitContexts[i];
    		
    		if (tmp.satisfiedBy(offer)) { 
    			return true;
    		}
    	}
    	
    	return false;
    }
    
    private boolean satisfiedBy(OrWorkerContext offer) {
    	
    	UnitWorkerContext [] tmp = offer.getContexts();
    	
    	for (int i=0;i<tmp.length;i++) { 
    		
    		if (satisfiedBy(tmp[i])) { 
    			return true;
    		}
    	}

    	return false;
    }
    
    @Override
    public boolean satisfiedBy(WorkerContext offer) {
        
        if (offer == null) {
            return false;
        }
        
        if (offer.isUnit()) {
        	return satisfiedBy((UnitWorkerContext) offer);
        }
        
        if (offer.isOr()) {
        	return satisfiedBy((OrWorkerContext) offer);
        }
        
        return false;
    }

    /*
    public static Context merge(Context[] a) {

        // TODO: slow N^2 implementation!!!!

        if (a == null || a.length == 0) { 
            return UnitContext.DEFAULT;
        }
        
        LinkedHashMap<String name, >
        
        ArrayList<UnitContext> unit = new ArrayList<UnitContext>();
        
        for (int i=0;i<a.length;i++) {
            Context tmp = a[i];
            
            if (tmp != null) { 

                if (tmp.isUnit()) {
                    if (!unit.contains(tmp)) { 
                        unit.add((UnitContext) tmp);
                    }
                } else if (tmp.isOr()) { 
                    
                    UnitContext [] uc = ((OrContext)tmp).unitContexts;
                    
                    for (int j=0;i<uc.length;j++) { 
                        
                        UnitContext u = uc[i];
                        
                        if (!unit.contains(u)) { 
                            unit.add(u);
                        }
                    }
                }
            }
        }
  
        if (unit.size() == 0) { 
            
            int size = and.size();
            
            if (size == 0) { 
                return UnitContext.DEFAULT;
            }
        
            if (size == 1) { 
                return and.get(0);
            }
    
        } else if (unit.size() == 1) { 
            
            if (and.size() == 0) { 
                return unit.get(0);
            }
        }   
        
        return new OrContext(unit.toArray(new UnitContext[unit.size()]); 

    }
    */
}
