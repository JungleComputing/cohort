package ibis.cohort.impl.distributed;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.CohortIdentifier;
import ibis.cohort.Context;

public interface BottomCohort {

    /* 
     * This interface contains all methods a super cohort will invoke on 
     * it's sub cohort(s). 
     */
    
    /* synchronous methods - immediately produce a result or effect */ 
    void setContext(CohortIdentifier id, Context context) throws Exception;
    Context getContext();
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
