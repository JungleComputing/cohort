package test.spawntest;

import ibis.constellation.Activity;
import ibis.constellation.ActivityIdentifier;
import ibis.constellation.Event;
import ibis.constellation.MessageEvent;
import ibis.constellation.context.UnitActivityContext;

public class Dummy extends Activity {

    private static final long serialVersionUID = 5970093414747228592L;
    
    private final ActivityIdentifier parent;
    
    public Dummy(ActivityIdentifier parent) {
        super(UnitActivityContext.DEFAULT, false);
        this.parent = parent;
    }

    @Override
    public void initialize() throws Exception {
        executor.send(new MessageEvent(identifier(), parent, null));
        finish();
    }

    @Override
    public void process(Event e) throws Exception {
        // unused
    }

    @Override
    public void cleanup() throws Exception {
        // unused
    }

    @Override
    public void cancel() throws Exception {
        // TODO Auto-generated method stub
        
    }

}
