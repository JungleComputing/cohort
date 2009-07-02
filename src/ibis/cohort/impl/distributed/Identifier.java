package ibis.cohort.impl.distributed;

import ibis.cohort.ActivityIdentifier;
import ibis.ipl.IbisIdentifier;

public class Identifier extends ActivityIdentifier {
    
    private static final long serialVersionUID = -2306905988828294374L;
    
    private final IbisIdentifier ibis;
    private final int workerID;
    private final long id;

    protected Identifier(final IbisIdentifier ibis, final int workerID, 
            final long id) {
        this.ibis = ibis;
        this.workerID = workerID;
        this.id = id;
    }

    protected IbisIdentifier getIbis() { 
        return ibis;
    }
    
    protected int getWorkerID() { 
        return workerID;
    }
    
    public String toString() { 
        return "Activity(" + ibis + ", " + id + ")";
    }

    @Override
    public int hashCode() {
       return ibis.hashCode() + (int) id;
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
