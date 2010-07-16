package ibis.cohort.context;

import ibis.cohort.Context;

public class AnyContext extends Context {
    
    private static final long serialVersionUID = 8675999773993507080L;

    @Override
    public boolean equals(Object other) {
        return other instanceof AnyContext;
    }
    
    @Override
    public boolean isAny() { 
        return true;
    }

    @Override
    public boolean contains(Context other) {
        return true;
    }
   
    public String toString() { 
        return "ANYContext";
    }

    @Override
    public boolean satisfiedBy(Context other) {
        return true;
    }
}
