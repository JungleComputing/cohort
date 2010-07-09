package test.pipeline.inbalance;

import ibis.cohort.ActivityIdentifier;
import ibis.cohort.SimpleActivity;
import ibis.cohort.context.UnitContext;

public class Stage4 extends SimpleActivity {
    
    private static final long serialVersionUID = 8685301161185498131L;

    private final ActivityIdentifier target;
    private final long sleep;
    private final Data data;
    
    public Stage4(ActivityIdentifier target, long sleep, Data data) { 
        
        super(new UnitContext("C"));
  
        this.target = target;
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
        
        cohort.send(identifier(), target, new Data(data.index, 4, data.data));
    }
}
