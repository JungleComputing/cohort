package ibis.cohort.impl.distributed.single;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Properties;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.ActivityIdentifierFactory;
import ibis.cohort.Cohort;
import ibis.cohort.CohortIdentifier;
import ibis.cohort.Context;
import ibis.cohort.Event;
import ibis.cohort.extra.CohortIdentifierFactory;
import ibis.cohort.extra.Log;
import ibis.cohort.extra.SimpleCohortIdentifierFactory;
import ibis.cohort.impl.distributed.ActivityRecord;
import ibis.cohort.impl.distributed.ApplicationMessage;
import ibis.cohort.impl.distributed.BottomCohort;
import ibis.cohort.impl.distributed.LookupReply;
import ibis.cohort.impl.distributed.LookupRequest;
import ibis.cohort.impl.distributed.StealReply;
import ibis.cohort.impl.distributed.StealRequest;
import ibis.cohort.impl.distributed.TopCohort;
import ibis.cohort.impl.distributed.UndeliverableEvent;

public class SingleThreadedTopCohort extends Thread implements Cohort, TopCohort {
    
    private static final boolean DEBUG = true;
    private static final boolean PROFILE = true;
    
    private final CohortIdentifier identifier = new CohortIdentifier(0);
    
    private Context myContext;
    
    private BaseCohort sequential;
    
    private PrintStream out; 
    private Log logger;
        
    public SingleThreadedTopCohort(Properties p) {
        
        super("SingleThreadedCohort CID: 0x0");
        
        System.out.println("Starting SingleThreadedCohort " + identifier);
      
        myContext = Context.ANY;

        String outfile = p.getProperty("ibis.cohort.outputfile");
        
        if (outfile != null) {
            String filename = outfile + "." + identifier;
            
            try {
                out = new PrintStream(new BufferedOutputStream(
                        new FileOutputStream(filename)));
            } catch (Exception e) {
                System.out.println("Failed to open output file " + outfile);
                out = System.out;
            }
            
        } else { 
            out = System.out;
        }
        
        logger = new Log(identifier + " [ST] ", out, DEBUG);
        
        //sequential = new BaseCohort(this, p, identifier, out, logger);
    }
    
    
    
    
    public boolean activate() {
        // TODO Auto-generated method stub
        return false;
    }

    public void cancel(ActivityIdentifier activity) {
        // TODO Auto-generated method stub
        
    }

    public boolean deregister(String name, Context scope) {
        // TODO Auto-generated method stub
        return false;
    }

    public void done() {
        // TODO Auto-generated method stub
        
    }

    public Context getContext() {
        // TODO Auto-generated method stub
        return null;
    }

    public Cohort[] getSubCohorts() {
        // TODO Auto-generated method stub
        return null;
    }

    public CohortIdentifier identifier() {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean isMaster() {
        // TODO Auto-generated method stub
        return false;
    }

    public ActivityIdentifier lookup(String name, Context scope) {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean register(String name, ActivityIdentifier id, Context scope) {
        // TODO Auto-generated method stub
        return false;
    }

    public void send(ActivityIdentifier source, ActivityIdentifier target, Object o) {
        // TODO Auto-generated method stub
        
    }

    public void send(Event e) {
        // TODO Auto-generated method stub
        
    }

    public void setContext(Context context) throws Exception {
        // TODO Auto-generated method stub
        
    }

    public void setContext(CohortIdentifier id, Context context) throws Exception {
        // TODO Auto-generated method stub
        
    }

    public ActivityIdentifier submit(Activity job) {
        // TODO Auto-generated method stub
        return null;
    }

    public void contextChanged(CohortIdentifier cid, Context newContext) {
        // TODO Auto-generated method stub
        
    }

    public ActivityIdentifierFactory getActivityIdentifierFactory(CohortIdentifier cid) {
        // TODO Auto-generated method stub
        return null;
    }

    public CohortIdentifierFactory getCohortIdentifierFactory(CohortIdentifier cid) {
        // TODO Auto-generated method stub
        return null;
    }

    public void handleApplicationMessage(ApplicationMessage m) {
        // TODO Auto-generated method stub
        
    }

    public LookupReply handleLookup(LookupRequest lr) {
        // TODO Auto-generated method stub
        return null;
    }

    public void handleLookupReply(LookupReply m) {
        // TODO Auto-generated method stub
        
    }

    public void handleStealReply(StealReply m) {
        // TODO Auto-generated method stub
        
    }

    public ActivityRecord handleStealRequest(StealRequest sr) {
        // TODO Auto-generated method stub
        return null;
    }

    public void handleUndeliverableEvent(UndeliverableEvent m) {
        // TODO Auto-generated method stub
        
    }

    public void handleWrongContext(ActivityRecord ar) {
        // TODO Auto-generated method stub
        
    }

}
