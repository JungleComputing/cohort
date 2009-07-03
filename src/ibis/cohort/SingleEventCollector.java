package ibis.cohort;

public class SingleEventCollector extends Activity {

    private static final long serialVersionUID = -538414301465754654L;
   
    private Event event;
    
    @Override
    public void initialize() throws Exception {
        suspend();
    }

    public SingleEventCollector() {
        super(Context.HERE);
    }
    
    @Override
    public synchronized void process(Event e) throws Exception {
      
        System.out.println("GOT RESULT!");
        
        event = e;
        notifyAll();
        finish();
    }

    public void cleanup() throws Exception {
        // empty
    }
    
    public void cancel() throws Exception {
        // empty
    }
    
    public String toString() { 
        return "SingleEventCollector(" + identifier() + ")";
    }
    
    public synchronized Event waitForEvent() { 
        while (event == null) {
            try { 
                wait();
            } catch (Exception e) {
                // ignore
            }
        }

        return event;
    }
}
