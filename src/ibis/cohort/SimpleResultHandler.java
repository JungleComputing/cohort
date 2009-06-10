package ibis.cohort;

public class SimpleResultHandler implements ResultHandler {

    private Object result;

    public void storeResult(Job job, Object result) {
 //       System.out.println("Got final result");
        
        synchronized (this) { 
        	this.result = result;
        	notifyAll();
        }
    }

    public synchronized Object waitForResult() { 
        while (result == null) {
            try { 
                wait();
            } catch (Exception e) {
                // ignore
            }
        }

        return result;
    }
}
