package ibis.cohort.impl.multithreaded;

import ibis.cohort.CohortIdentifier;

class MTCohortIdentifier extends CohortIdentifier {

    private static final long serialVersionUID = -5441833960431997656L;
    
    private final int identifier;
    
    MTCohortIdentifier(final int identifier) { 
        this.identifier = identifier;
    }

    int getIdentifier() { 
        return identifier;
    }
    
    @Override
    public int hashCode() {
        return 31 + identifier;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final MTCohortIdentifier other = (MTCohortIdentifier) obj;
        
        return (identifier == other.identifier);
    }
    
    public String toString() { 
        return "Cohort-" + identifier;
    }
    
}
