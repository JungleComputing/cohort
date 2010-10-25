package ibis.constellation;

import ibis.constellation.impl.DistributedConstellation;
import ibis.constellation.impl.MultiThreadedConstellation;
import ibis.constellation.impl.SingleThreadedConstellation;

import java.util.Properties;

public class ConstellationFactory {
/*
    public static String SEPARATOR = "+";
    
    public static Cohort createCohort() throws Exception{ 
        return createCohort(System.getProperties());                
    }
    
    private static TopCohort createTop(String name, Properties p, WorkerContext context) throws Exception { 
        
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
    
    private static WorkerContext parseOr(String c) {
        
        StringTokenizer tok = new StringTokenizer(c, "^");
        
        ArrayList<UnitWorkerContext> unit = new ArrayList<UnitWorkerContext>();
          
        // TODO: add range etc.
        while (tok.hasMoreTokens()) { 
            unit.add(new UnitWorkerContext(tok.nextToken()));
        }
        
        UnitWorkerContext [] u = unit.toArray(new UnitWorkerContext[unit.size()]);
        
        return new OrWorkerContext(u, true);
    }
        
    private static BottomCohort createBottomCohort(String name, 
            TopCohort parent, Properties p) throws Exception {

        WorkerContext context = UnitWorkerContext.DEFAULT;
        
        if (name.contains(":")) { 
            String c = name.substring(name.indexOf(":")+1);
            name = name.substring(0, name.indexOf(":"));
   
            if (c.contains("^")) { 
                context = parseOr(c);
            } else { 
                context = new UnitWorkerContext(c);
            }
        }
        
        if (name.equals("st") || name.equals("singlethreaded")) { 
            return new SingleThreadedBottomCohort(parent, p, context);                
        } else if (name.equals("mt") || name.equals("multithreaded")) { 
            if (context != UnitWorkerContext.DEFAULT) { 
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
        WorkerContext context = UnitWorkerContext.DEFAULT;
        
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
*/
	
	public static Constellation createCohort(Executor e) throws Exception { 
		return createCohort(System.getProperties(), e);
	}
	
	public static Constellation createCohort(Properties p, Executor e) throws Exception { 
		return createCohort(p, new Executor [] { e });
	}
	
	public static Constellation createCohort(Executor ... e) throws Exception { 
		
		return createCohort(System.getProperties(), e);
	}
		
    public static Constellation createCohort(Properties p, Executor ... e) throws Exception { 
    	
    	if (e == null || e.length == 0) { 
    		throw new IllegalArgumentException("Need at least one executor!");
    	}
    	
    	// TODO: check is we need to create a new dist/mt here!!!
        DistributedConstellation d = new DistributedConstellation(p);
        MultiThreadedConstellation m = new MultiThreadedConstellation(d, p);                
          	
    	for (int i=0;i<e.length;i++) { 
    	    new SingleThreadedConstellation(m, e[i], p);          
    	}
    	
    	return d;
    }

}
