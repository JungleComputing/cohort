package ibis.cohort;

import java.io.Serializable;

public abstract class Identifier implements Serializable {

    /*
    private static long nextID = 0;

    private final long id;

    private Identifier(long id) { 
        this.id = id;
    }

    public static synchronized Identifier getNext() {
        return new Identifier(nextID++); 
    }

    public String toString() { 
        return "Job-" + id;
    }
*/
}
