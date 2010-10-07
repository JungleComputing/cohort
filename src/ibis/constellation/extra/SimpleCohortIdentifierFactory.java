package ibis.constellation.extra;

import ibis.constellation.CohortIdentifier;

public class SimpleCohortIdentifierFactory implements CohortIdentifierFactory {

    private long count; 
    
    public synchronized CohortIdentifier generateCohortIdentifier() {
        return new CohortIdentifier(count++);
    }
}
