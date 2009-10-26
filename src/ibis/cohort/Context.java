package ibis.cohort;

import java.io.Serializable;

public class Context implements Serializable {

    private static final long serialVersionUID = -2442900962246421740L;

    public final static Context LOCAL = new Context("local", false, true);
    public final static Context ANY   = new Context("any", true, false);

    public final String name; 
    public final boolean isAny; 
    public final boolean isLocal; 
    
    private Context(String name, boolean isAny, boolean isLocal) { 
        this.name = name;
        this.isAny = isAny;
        this.isLocal = isLocal;
    }
    
    public Context(String name) { 
        this.name = name;
        
        isAny = name.equals(ANY.name);
        isLocal = name.equals(LOCAL.name);
    }
    
    public boolean match(Context other) {
        
        if (isAny || other.isAny) { 
            return true;
        }
        
        if (isLocal && other.isLocal) { 
            return true;
        }
        
        return name.equals(other.name);
    }
    
    public String toString() { 
        return name;
    }
}
