package ibis.cohort.extra;

public class WorkQueueFactory {

    public static WorkQueue createQueue(String type, boolean sync) throws Exception { 
        
        if (type == null) { 
            type = "smart";
        }
        
        WorkQueue result = null;
        
        if (type.equals("smart")) { 
            result = new SmartWorkQueue();
        } else if (type.equals("simple")) { 
            result = new SimpleWorkQueue();
        } else if (type.equals("optsimple")) { 
            result = new OptimizedSimpleWorkQueue();
        } else {
            throw new Exception("Unknown workqueue type: " + type);
        }
        
        if (sync) { 
            result = new SynchronizedWorkQueue(result);
        }
        
        return result;
    } 
}
