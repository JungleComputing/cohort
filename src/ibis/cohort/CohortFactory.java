package ibis.cohort;

import ibis.cohort.impl.distributed.dist.DistributedCohort;
import ibis.cohort.impl.distributed.multi.MultiThreadedTopCohort;

import java.util.Properties;

public class CohortFactory {

    public static Cohort createCohort() throws Exception{ 
        return createCohort(System.getProperties());                
    }
    
    public static Cohort createCohort(Properties p) throws Exception { 
     
        // TODO: make more flexible!
        
        if (p != null) { 
            
            String name = p.getProperty("ibis.cohort.impl"); 
            
            if (name == null) { 
                // fall through                
          //  } else if (name.equals("seq") || name.equals("sequential")){ 
          //      return new Sequential();
            } else if (name.equals("mt") || name.equals("multithreaded")) { 
                return new MultiThreadedTopCohort(p, 0);                
            } else if (name.equals("dist") || name.equals("distributed")) { 
                return new DistributedCohort(p);
            } else { 
                throw new Exception("Unknown Cohort implementation \"" + name 
                        + "\" selected");
            }
        }
        
        throw new Exception("No Cohort implementation selected");        
    }      
}
