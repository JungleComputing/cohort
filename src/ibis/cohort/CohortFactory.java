package ibis.cohort;

import ibis.cohort.context.AndContext;
import ibis.cohort.context.OrContext;
import ibis.cohort.context.UnitContext;
import ibis.cohort.impl.distributed.BottomCohort;
import ibis.cohort.impl.distributed.TopCohort;
import ibis.cohort.impl.distributed.dist.DistributedCohort;
import ibis.cohort.impl.distributed.multi.MultiThreadedMiddleCohort;
import ibis.cohort.impl.distributed.multi.MultiThreadedTopCohort;
import ibis.cohort.impl.distributed.single.SingleThreadedBottomCohort;
import ibis.cohort.impl.distributed.single.SingleThreadedTopCohort;

import java.util.ArrayList;
import java.util.Properties;
import java.util.StringTokenizer;

public class CohortFactory {

    public static String SEPARATOR = "+";
    
    public static Cohort createCohort() throws Exception{ 
        return createCohort(System.getProperties());                
    }
    
    private static TopCohort createTop(String name, Properties p, Context context) throws Exception { 
        
        if (name == null) { 
            // fall through                
        } else if (name.equals("st") || name.equals("singlethreaded")) { 
            return new SingleThreadedTopCohort(p, context);                
        } else if (name.equals("mt") || name.equals("multithreaded")) { 
            return new MultiThreadedTopCohort(p);                
        } else if (name.equals("dist") || name.equals("distributed")) { 
            return new DistributedCohort(p);
        }
        
        throw new Exception("Unknown Cohort implementation \"" + name 
                    + "\" selected");
    }
    
    private static Context parseOr(String c) {
        
        StringTokenizer tok = new StringTokenizer(c, "^");
        
        ArrayList<UnitContext> unit = new ArrayList<UnitContext>();
        ArrayList<AndContext> and = new ArrayList<AndContext>();
          
        while (tok.hasMoreTokens()) { 
            
            String t = tok.nextToken();
            
            if (t.contains("*")) {
                and.add(parseAnd(t));
            } else {
                unit.add(new UnitContext(t));
            }
        }
        
        UnitContext [] u = unit.toArray(new UnitContext[unit.size()]);
        AndContext [] a = and.toArray(new AndContext[and.size()]);
        
        return new OrContext(u, a);
    }
    
    private static AndContext parseAnd(String c) {
        
        ArrayList<UnitContext> unit = new ArrayList<UnitContext>();
        
        StringTokenizer tok = new StringTokenizer(c, "*");
        
        while (tok.hasMoreTokens()) { 
            unit.add(new UnitContext(tok.nextToken()));
        }
        
        UnitContext [] u = unit.toArray(new UnitContext[unit.size()]);

        return new AndContext(u);
    }
    
    
    private static BottomCohort createBottomCohort(String name, 
            TopCohort parent, Properties p) throws Exception {

        Context context = UnitContext.DEFAULT;
        
        if (name.contains(":")) { 
            String c = name.substring(name.indexOf(":")+1);
            name = name.substring(0, name.indexOf(":"));
   
            if (c.contains("^")) { 
                context = parseOr(c);
            } else if (c.contains("*")) { 
                context = parseAnd(c);
            } else { 
                context = new UnitContext(c);
            }
        }
        
        if (name.equals("st") || name.equals("singlethreaded")) { 
            return new SingleThreadedBottomCohort(parent, p, context);                
        } else if (name.equals("mt") || name.equals("multithreaded")) { 
            if (context != UnitContext.DEFAULT) { 
                throw new Exception("Setting context for multithreaded cohort has no effect");
            }
            return new MultiThreadedMiddleCohort(parent, p);                
        } 
    
        throw new Exception("Unknown BottomCohort implementation \"" + name 
                    + "\" selected");
    }
        
    
    private static Cohort createCohortConfiguration(String config, Properties p) throws Exception {
  
        StringTokenizer st = new StringTokenizer(config, SEPARATOR);
     
        int tokens = st.countTokens();
        
        String name = st.nextToken();
        Context context = UnitContext.DEFAULT;
        
        if (name.contains(":")) { 
            String c = name.substring(name.indexOf(":")+1);
            name = name.substring(0, name.indexOf(":"));
        }
        
        TopCohort top = createTop(name, p, context);
        
        TopCohort current = top;
        
        for (int i=1;i<tokens;i++) { 
            
            name = st.nextToken();
            
            if (name.startsWith("[")) { 
            
                StringTokenizer tok2 = new StringTokenizer(name, " ,[]");
                
                while (tok2.hasMoreTokens()) { 
                    name = tok2.nextToken();
                   
                    BottomCohort bottom = createBottomCohort(name, current, p);
                }
                
            } else { 
                BottomCohort bottom = createBottomCohort(name, current, p);
                
                if (i != tokens-1) { 
                    current = (TopCohort) bottom;
                }
            }
        }
    
        return (Cohort) top;
    }

    private static String getSingleThreadedWorkers(Properties p) { 

        String tmp = p.getProperty("ibis.cohort.workers");

        int count = 0;
        
        if (tmp != null && tmp.length() > 0) {
            try {
                count = Integer.parseInt(tmp);
            } catch (Exception e) {
                System.err.println("Failed to parse property " +
                        "ibis.cohort.workers: " + e);
            }
        }

        if (count == 0) {
            // Automatically determine the number of cores to use
            count = Runtime.getRuntime().availableProcessors();
        }

        String config = "[st:any";
        
        for (int i=1;i<count;i++) { 
            config += ", st:any";
        }
       
        config += "]";

        return config;
    }
    
    private static String check(String name, Properties p) throws Exception { 
        
        if (name.equals("st") || 
                name.equals("singlethreaded") || 
                name.startsWith("st:") || 
                name.startsWith("singlethreaded:")) { 
            return name;
        } 
  
        if (name.equals("mt") || name.equals("multithreaded")) { 
            return "mt" + SEPARATOR + getSingleThreadedWorkers(p);
        }
        
        if (name.equals("dist") || name.equals("distributed")) { 
            return "dist" + SEPARATOR + "mt" + SEPARATOR + getSingleThreadedWorkers(p);
        } 
        
        throw new Exception("Unknown Cohort implementation \"" + name 
                    + "\" selected");
    }
    
    public static Cohort createCohort(Properties p) throws Exception { 

        if (p != null) { 

            String name = p.getProperty("ibis.cohort.impl"); 

            if (name != null) { 
              
                if (!name.contains(SEPARATOR)) {
                    name = check(name, p);
                }   
 
                System.out.println("CREATING COHORT: " + name);
                
                return createCohortConfiguration(name, p);
            }
        }
        
        throw new Exception("No Cohort implementation selected");        
    }


}
