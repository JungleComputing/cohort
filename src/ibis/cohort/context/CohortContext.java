package ibis.cohort.context;

import ibis.cohort.Context;

public class CohortContext extends Context {

    private static final long serialVersionUID = -5232202167241369014L;

    @Override
    public boolean equals(Object other) {
        return other instanceof CohortContext;
    }
    
    @Override
    public boolean isCohort() { 
        return true;
    }

    @Override
    public boolean contains(Context other) {
        return equals(other);
    }
}
