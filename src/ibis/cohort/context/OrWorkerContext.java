package ibis.cohort.context;

import ibis.cohort.WorkerContext;

public class OrWorkerContext extends WorkerContext {

	private static final long serialVersionUID = -1202476921345669674L;

    protected final UnitWorkerContext [] unitContexts;
    
    protected final boolean ordered;    
	
	public OrWorkerContext(UnitWorkerContext[] unit, boolean ordered) {        
        super();
      
        if (unit == null || unit.length < 2) { 
        	  throw new IllegalArgumentException("Invalid arguments to " +
                      "OrContext: 2 or more contexts required!");
        }
    
        unitContexts = unit;
        this.ordered = ordered;
	}

	public int size() {
		return unitContexts.length;
	}

	public UnitWorkerContext get(int index) {
		return unitContexts[index];
	}
	
	public UnitWorkerContext[] getContexts() {
		return unitContexts.clone();
	}

	public boolean isOrdered() { 
		return ordered;
	}
	
}
