package ibis.constellation;

import ibis.constellation.context.UnitActivityContext;

public class MultiEventCollector extends Activity {

    private static final long serialVersionUID = -538414301465754654L;
   
    private final Event [] events;
    private int count;
    
    public MultiEventCollector(ActivityContext c, int events) {
        super(c, true);
        this.events = new Event[events];
    }
    
    
    public MultiEventCollector(int events) {
        this(UnitActivityContext.DEFAULT, events);
    }
    
    @Override
    public void initialize() throws Exception {
        suspend();
    }
    
    @Override
    public synchronized void process(Event e) throws Exception {

    	System.out.println("MultiEventCollector: received event " 
    			+ count + " of " + events.length);
    	
        events[count++] = e;
        
        if (count == events.length) { 
            notifyAll();
            finish();
        } else { 
            suspend();
        }           
    }

    public void cleanup() throws Exception {
        // empty
    }
    
    public void cancel() throws Exception {
        // empty
    }
    
    public String toString() { 
        return "MultiEventCollector(" + identifier() + ", " 
            + events.length + ")";
    }
    
    public synchronized Event [] waitForEvents() { 
        while (count != events.length) {
            try { 
                wait();
            } catch (Exception e) {
                // ignore
            }
        }

        return events;
    }
}
