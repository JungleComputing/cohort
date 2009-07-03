package ibis.cohort.impl.distributed;

import ibis.cohort.ActivityIdentifier;
import ibis.ipl.IbisIdentifier;

public class DistributedActivityIdentifier extends ActivityIdentifier {
    
    private static final long serialVersionUID = -2306905988828294374L;
    
    private final DistributedCohortIdentifier cohort;
    private final long id;

    protected DistributedActivityIdentifier(final DistributedCohortIdentifier cohort, final long id) {
        this.cohort = cohort;
        this.id = id;
    }

    protected DistributedCohortIdentifier getCohort() { 
        return cohort;
    }
    
    public String toString() { 
        return "Activity(" + cohort + ", " + id + ")";
    }

    @Override
    public int hashCode() {
       return cohort.hashCode() + (int) id;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        
        final DistributedActivityIdentifier other = (DistributedActivityIdentifier) obj;
      
        if (id != other.id) { 
            return false;
        }
        
        return cohort.equals(other.cohort);
    }
}
