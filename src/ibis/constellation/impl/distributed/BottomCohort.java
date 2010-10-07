package ibis.constellation.impl.distributed;

import ibis.constellation.Activity;
import ibis.constellation.ActivityIdentifier;
import ibis.constellation.CohortIdentifier;
import ibis.constellation.WorkerContext;

public interface BottomCohort {

    /* 
     * This interface contains all methods a super cohort will invoke on 
     * it's sub cohort(s). 
     */
    
    /* synchronous methods - immediately produce a result or effect */ 
   
    CohortIdentifier [] getLeafIDs();
    void setContext(CohortIdentifier id, WorkerContext context) throws Exception;
    WorkerContext getContext();
    CohortIdentifier identifier();
    boolean activate();
    void done();
    boolean canProcessActivities();
    ActivityIdentifier deliverSubmit(Activity a);
    
//    void deliverCancel(ActivityIdentifier aid);
//    void deliverStealReply(StealReply sr);
    
    /* asynchronous methods - the effect will not be immediate, any results are 
     *                        delivered via a callback method */
    //void deliverSteal(StealRequest sr);
    //void deliverLookup(LookupRequest lr);
   
    //void deliverEvent(Event e);
  
    //void deliverApplicationMessage(ApplicationMessage m);
    
    //void deliverMessage(Message m);
    
    void deliverStealRequest(StealRequest sr);
    void deliverLookupRequest(LookupRequest lr);
    void deliverStealReply(StealReply sr);
    void deliverLookupReply(LookupReply lr);
    void deliverEventMessage(ApplicationMessage m);
    void deliverUndeliverableEvent(UndeliverableEvent m);
    
}
