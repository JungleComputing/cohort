package test.pipeline.simple;

import ibis.cohort.ActivityIdentifier;
import ibis.cohort.SimpleActivity;
import ibis.cohort.context.UnitActivityContext;

public class Pipeline extends SimpleActivity {

    private static final long serialVersionUID = -3987089095770723454L;
   
    private final int index;
    private final int current;
    private final int last;
    private final long sleep;
    private final Object data;
    
    public Pipeline(ActivityIdentifier parent, int index, int current, int last, long sleep, Object data) { 
        
        super(parent, new UnitActivityContext("c" + current));
        
        this.index = index;
        this.current = current;
        this.last = last;
        this.sleep = sleep;
        this.data = data;
    }
    
    @Override
    public void simpleActivity() throws Exception {
   
        System.out.println("RUNNING pipeline " + index + " " + current + " " + last);
        
        if (sleep > 0) { 
            try { 
                Thread.sleep(sleep);
            } catch (Exception e) {
                // ignored
            }
        }
        
        if (current == last) { 
        
            System.out.println("Sending pipeline reply");
            
            cohort.send(identifier(), parent, data);
        } else { 

            System.out.println("Submitting pipeline stage: " + index + " " + (current+1) + " " + last);
        
            cohort.submit(new Pipeline(parent, index, current+1, last, sleep, data));
        }        
    }
}
