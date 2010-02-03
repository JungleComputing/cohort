package ibis.cohort.impl.distributed;

import ibis.cohort.ActivityIdentifierFactory;
import ibis.cohort.CohortIdentifier;
import ibis.cohort.Context;
import ibis.cohort.extra.CohortIdentifierFactory;

public interface TopCohort {
    /* 
     * This interface contains all methods a sub cohort will invoke on 
     * it's super cohort. 
     */
    
    /* synchronous methods - immediately produce a result or effect */ 
    void contextChanged(CohortIdentifier cid, Context newContext);
    
    ActivityIdentifierFactory getActivityIdentifierFactory(CohortIdentifier cid);
    CohortIdentifierFactory getCohortIdentifierFactory(CohortIdentifier cid);
    
    void handleWrongContext(ActivityRecord ar);
    
    /* weak asynchronous methods - part of the effect may be posponed */
    
    /*
    void forwardStealReply(StealReply sr);
    void forwardLookupReply(LookupReply lr);
    void handleUndeliverableEvent(UndeliverableEvent e);
    */
    
    ActivityRecord handleStealRequest(StealRequest sr);
    LookupReply handleLookup(LookupRequest lr);
    
    /* asynchronous methods - the effect will not be immediate, any results are 
     *                        delivered via a callback method */
    
    void handleApplicationMessage(ApplicationMessage m);
    void handleLookupReply(LookupReply m);
    void handleStealReply(StealReply m);
    void handleUndeliverableEvent(UndeliverableEvent m);
}

