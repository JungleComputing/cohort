package ibis.cohort.extra;

public class WorkQueueFactory {

    public static WorkQueue createQueue(String type, boolean sync, String id) throws Exception { 
        
        if (type == null) { 
            type = "smartsorted";
        }
        
        WorkQueue result = null;
        
        if (type.equals("smartsorted")) { 
            result = new SmartSortedWorkQueue(id, new ActivitySizeComparator());
        } else if (type.equals("smart")) { 
            result = new SmartWorkQueue(id);
        } else if (type.equals("simple")) { 
            result = new SimpleWorkQueue(id);
        } else if (type.equals("optsimple")) { 
            result = new OptimizedSimpleWorkQueue(id);
        } else {
            throw new Exception("Unknown workqueue type: " + type);
        }
        
        if (sync) { 
            result = new SynchronizedWorkQueue(result);
        }
        
        return result;
    } 
}
