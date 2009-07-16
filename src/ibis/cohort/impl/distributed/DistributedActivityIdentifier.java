package ibis.cohort.impl.distributed;

import ibis.cohort.ActivityIdentifier;

public class DistributedActivityIdentifier extends ActivityIdentifier {
    
    private static final long serialVersionUID = -2306905988828294374L;
    
    private final DistributedCohortIdentifier cohortOfOrigin;
    private final long id;

    private DistributedCohortIdentifier lastKnownCohort; 
    
    protected DistributedActivityIdentifier(final DistributedCohortIdentifier cohort, final long id) {
        this.cohortOfOrigin = cohort;
        this.id = id;
    }
    
    protected DistributedCohortIdentifier getLastKnownCohort() { 
        if (lastKnownCohort == null) { 
            return cohortOfOrigin;
        }
        
        return lastKnownCohort;
    }
     
    protected void setLastKnownCohort(DistributedCohortIdentifier id) { 
        lastKnownCohort = id;
    }
    
    protected DistributedCohortIdentifier getCohort() { 
        return cohortOfOrigin;
    }
    
    public String toString() { 
        return "Activity(origin: " + cohortOfOrigin + " last: " + 
            lastKnownCohort + " id: " + id + ")";
    }

    @Override
    public int hashCode() {
       return cohortOfOrigin.hashCode() + (int) id;
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
        
        return cohortOfOrigin.equals(other.cohortOfOrigin);
    }
}
