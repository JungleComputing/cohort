package test.pipeline.inbalance;

import ibis.constellation.ActivityIdentifier;
import ibis.constellation.MessageEvent;
import ibis.constellation.SimpleActivity;
import ibis.constellation.context.UnitActivityContext;

public class Stage4 extends SimpleActivity {
    
    private static final long serialVersionUID = 8685301161185498131L;

    private final long sleep;
    private final Data data;
    
    public Stage4(ActivityIdentifier target, long sleep, Data data) { 
        
        super(target, new UnitActivityContext("C"));
  
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
        
        executor.send(new MessageEvent(identifier(), parent, new Data(data.index, 4, data.data)));
    }
}

