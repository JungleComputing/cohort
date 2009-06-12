package ibis.cohort.impl;

import ibis.cohort.Identifier;

public class SequentialIdentifier extends Identifier {
    
    private static final long serialVersionUID = -2306905988828294374L;
    
    private final long id;

    protected SequentialIdentifier(long id) { 
        this.id = id;
    }

    public String toString() { 
        return "Activity-" + id;
    }

    @Override
    public int hashCode() {
       return (int) id;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final SequentialIdentifier other = (SequentialIdentifier) obj;
      
        return (id == other.id);
    }
}
