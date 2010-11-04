package test.spawntest;

import ibis.constellation.ActivityIdentifier;
import ibis.constellation.Event;
import ibis.constellation.SimpleActivity;
import ibis.constellation.context.UnitActivityContext;

public class Dummy extends SimpleActivity {

    private static final long serialVersionUID = 5970093414747228592L;
    
    public Dummy(ActivityIdentifier parent) {
        super(parent, UnitActivityContext.DEFAULT);
        this.parent = parent;
    }

	@Override
	public void simpleActivity() throws Exception {
        executor.send(new Event(identifier(), parent, null));
        finish();
	}

}
