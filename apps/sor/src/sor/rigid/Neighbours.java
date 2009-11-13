package sor.rigid;

import ibis.cohort.ActivityIdentifier;

import java.io.Serializable;

public class Neighbours implements Serializable {

    private static final long serialVersionUID = -8081244944026816300L;
 
    public final ActivityIdentifier previous; 
    public final ActivityIdentifier next;
    
    public Neighbours(final ActivityIdentifier previous, 
            final ActivityIdentifier next) {
        super();
        this.previous = previous;
        this.next = next;
    }    
}
