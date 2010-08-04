package ibis.cohort;

import java.io.Serializable;

public class CohortIdentifier implements Serializable {

    private static final long serialVersionUID = -8236873210293335756L;

    public final long id; 
    
    public CohortIdentifier(final long id) { 
        this.id = id;
    }
    
    @Override
    public int hashCode() {
        final int PRIME = 31;
        int result = 1;
        result = PRIME * result + (int) (id ^ (id >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        
        if (obj == null)
            return false;
        
        if (getClass() != obj.getClass())
            return false;
   
        final CohortIdentifier other = (CohortIdentifier) obj;
    
        return (id == other.id);
    }
    
    public String toString() { 
        return "CID: " + Long.toHexString(id);
    }
    
}
