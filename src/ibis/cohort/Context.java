package ibis.cohort;

import java.io.Serializable;

public class Context implements Serializable {

    private static final long serialVersionUID = -2442900962246421740L;

    public final static Context HERE       = new Context("here");
    public final static Context EVERYWHERE = new Context("everywhere");
    public final static Context ANYWHERE   = new Context("anywhere");

    public final String name; 

    public Context(String name) { 
        this.name = name;
    }
}
