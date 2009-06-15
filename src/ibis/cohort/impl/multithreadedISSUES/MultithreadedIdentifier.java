package ibis.cohort.impl.multithreadedISSUES;

import ibis.cohort.ActivityIdentifier;

public class MultithreadedIdentifier extends ActivityIdentifier {
    
    private static final long serialVersionUID = -2306905988828294374L;
    
    private final long id;

    protected MultithreadedIdentifier(long id) { 
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
        final MultithreadedIdentifier other = (MultithreadedIdentifier) obj;
      
        return (id == other.id);
    }
}
