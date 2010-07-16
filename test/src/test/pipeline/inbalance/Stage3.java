package test.pipeline.inbalance;

import ibis.cohort.ActivityIdentifier;
import ibis.cohort.SimpleActivity;
import ibis.cohort.context.UnitContext;

public class Stage3 extends SimpleActivity {

    private static final long serialVersionUID = 8278819333486874900L;
  
    private final long sleep;
    private final Data data;
    
    public Stage3(ActivityIdentifier target, long sleep, Data data) { 
        
        super(target, new UnitContext("B"));
  
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
        
        cohort.send(identifier(), parent, new Data(data.index, 3, data.data));
    }
}
