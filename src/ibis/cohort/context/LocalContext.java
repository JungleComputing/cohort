package ibis.cohort.context;

import ibis.cohort.Context;

public class LocalContext extends Context {
    
    private static final long serialVersionUID = -1606777642174100518L;

    @Override
    public boolean equals(Object other) {
        return other instanceof LocalContext;
    }
 
    @Override
    public boolean isLocal() { 
        return true;
    }

    @Override
    public boolean contains(Context other) {
        return equals(other);
    }
    
}
