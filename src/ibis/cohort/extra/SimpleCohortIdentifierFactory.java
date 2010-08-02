package ibis.cohort.extra;

import ibis.cohort.CohortIdentifier;

public class SimpleCohortIdentifierFactory implements CohortIdentifierFactory {

    private long count; 
    
    public synchronized CohortIdentifier generateCohortIdentifier() {
        return new CohortIdentifier(count++);
    }
}
