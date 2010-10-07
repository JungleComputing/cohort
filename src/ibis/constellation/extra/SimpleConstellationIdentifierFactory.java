package ibis.constellation.extra;

import ibis.constellation.ConstellationIdentifier;

public class SimpleConstellationIdentifierFactory implements ConstellationIdentifierFactory {

    private long count; 
    
    public synchronized ConstellationIdentifier generateCohortIdentifier() {
        return new ConstellationIdentifier(count++);
    }
}
