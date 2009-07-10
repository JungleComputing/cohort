package ibis.cohort;

import java.io.Serializable;

public class Context implements Serializable {

    private static final long serialVersionUID = -2442900962246421740L;

    public final static Context LOCAL = new Context("local");
    public final static Context ANY   = new Context("any");

    public final String name; 

    public Context(String name) { 
        this.name = name;
    }
}
