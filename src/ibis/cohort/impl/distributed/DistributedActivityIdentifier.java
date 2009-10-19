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
    
    public String localName() { 
        return cohortOfOrigin.simpleName() + ":" + id;
    }

    
    @Override
    public int hashCode() {
        final int PRIME = 31;
        int result = 1;
        result = PRIME * result + ((cohortOfOrigin == null) ? 0 : cohortOfOrigin.hashCode());
        result = PRIME * result + (int) (id ^ (id >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        
        if (getClass() != obj.getClass())
            return false;
        
        final DistributedActivityIdentifier other = (DistributedActivityIdentifier) obj;
       
        if (cohortOfOrigin == null) {
            if (other.cohortOfOrigin != null) { 
                return false;
            }
        } else {
            if (!cohortOfOrigin.equals(other.cohortOfOrigin)) {
                return false;
            }
        }
        
        return (id == other.id); 
    }
}
