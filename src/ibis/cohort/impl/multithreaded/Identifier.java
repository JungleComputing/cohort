package ibis.cohort.impl.multithreaded;

import ibis.cohort.ActivityIdentifier;

public class Identifier extends ActivityIdentifier {
    
    private static final long serialVersionUID = -2306905988828294374L;
    
    private final long id;

    protected Identifier(long id) { 
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
        final Identifier other = (Identifier) obj;
      
        return (id == other.id);
    }
}
