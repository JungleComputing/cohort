package ibis.cohort;

import java.io.Serializable;

public class ActivityIdentifier implements Serializable {

    private static final long serialVersionUID = 4785081436543353644L;

    // The globally unique UUID for this activity is "high:low" 
    public final long high;
    public final long low;
    
    public ActivityIdentifier(long high, long low) { 
        this.high = high;
        this.low = low;
    }
    
    @Override
    public int hashCode() {
        final int PRIME = 31;
        int result = 1;
        result = PRIME * result + (int) (high ^ (high >>> 32));
        result = PRIME * result + (int) (low ^ (low >>> 32));
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
    
        final ActivityIdentifier other = (ActivityIdentifier) obj;

        return (high == other.high && low == other.low);
    }

    public String toString() { 
        return "AID: " + Long.toHexString(high) + ":" + Long.toHexString(low);
    }
}
