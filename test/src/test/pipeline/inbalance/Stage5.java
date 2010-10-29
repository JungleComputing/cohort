package test.pipeline.inbalance;

import ibis.constellation.Activity;
import ibis.constellation.ActivityIdentifier;
import ibis.constellation.Event;
import ibis.constellation.MessageEvent;
import ibis.constellation.context.UnitActivityContext;

public class Stage5 extends Activity {

    private static final long serialVersionUID = -2003940189338627474L;
    
    private final ActivityIdentifier parent;
    private final long sleep;
   
    private Data result3;
    private Data result4;
 
    public Stage5(ActivityIdentifier parent, long sleep) { 
        
        super(new UnitActivityContext("A"), true);
   
        this.parent = parent;
        this.sleep = sleep;
    }

    @Override
    public void initialize() throws Exception {
        suspend();
    }
    
    @Override
    public void cancel() throws Exception {
        // Not used
    }

    @Override
    public void cleanup() throws Exception {
       
        Data result = processData();
        
        System.out.println("Finished pipeline: " + result.index);
        
        executor.send(new MessageEvent(identifier(), parent, result));
    }
  
    private Data processData() { 
        
        // Simulate some processing here that takes 'sleep' time
        if (sleep > 0) { 
            try { 
                Thread.sleep(sleep);
            } catch (Exception e) {
                // ignored
            }
        }
        
        return new Data(result3.index, 5, result3.data);
    }
    
    @Override
    public void process(Event e) throws Exception {

        Data data = (Data)((MessageEvent) e).message;
       
        if (data.stage == 3) { 
            result3 = data;
        } else { 
            result4 = data;
        }
        
        if (result3 != null && result4 != null) { 
            finish();
        } else { 
            suspend();
        }
    }
}
