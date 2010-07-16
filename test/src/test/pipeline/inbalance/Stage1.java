package test.pipeline.inbalance;

import ibis.cohort.ActivityIdentifier;
import ibis.cohort.SimpleActivity;
import ibis.cohort.context.UnitContext;

public class Stage1 extends SimpleActivity {

    private static final long serialVersionUID = -3987089095770723454L;
   
    private final long sleep;
    private final Data data;
    
    public Stage1(ActivityIdentifier parent, long sleep, Data data) { 
        
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
      
        cohort.submit(new Stage2(parent, 200, new Data(data.index, 1, data.data)));       
    }
}
