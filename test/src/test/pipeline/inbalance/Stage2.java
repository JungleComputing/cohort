package test.pipeline.inbalance;

import ibis.cohort.ActivityIdentifier;
import ibis.cohort.SimpleActivity;
import ibis.cohort.context.UnitContext;

public class Stage2 extends SimpleActivity {

    private static final long serialVersionUID = -3987089095770723454L;
    
    private final long sleep;
    private final Data data;
    
    public Stage2(ActivityIdentifier parent, long sleep, Data data) { 
        
        super(parent, new UnitContext("A"));
        this.sleep = sleep;
        this.data = data;
    }
    
    @Override
    public void simpleActivity() throws Exception {
   
        if (sleep > 0) { 
            try { 
                Thread.sleep(sleep);
            } catch (Exception e) {
                // ignored
            }
        }
      
        // Submit stage5 first, as it it used to gather the results from stage3&4 
        ActivityIdentifier id = cohort.submit(new Stage5(parent, 100));
        
        cohort.submit(new Stage3(id, 1000, data));       
        cohort.submit(new Stage4(id, 600, data));       
        
    }
}
