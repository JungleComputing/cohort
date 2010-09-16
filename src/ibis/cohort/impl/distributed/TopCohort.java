package ibis.cohort.impl.distributed;

import ibis.cohort.ActivityIdentifierFactory;
import ibis.cohort.CohortIdentifier;
import ibis.cohort.StealPool;
import ibis.cohort.WorkerContext;
import ibis.cohort.extra.CohortIdentifierFactory;
import ibis.cohort.impl.distributed.single.SingleThreadedBottomCohort;

public interface TopCohort {
    /* 
     * This interface contains all methods a sub cohort will invoke on 
     * it's super cohort. 
     */
    
    /* synchronous methods - immediately produce a result or effect */ 
    void contextChanged(CohortIdentifier cid, WorkerContext newContext);
    
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

    
    /* callback method - used to register new cohorts */
    void register(BottomCohort cohort) throws Exception;

	void registerPool(SingleThreadedBottomCohort singleThreadedBottomCohort,
			StealPool oldPool, StealPool newPool);

	void registerStealPool(
			SingleThreadedBottomCohort singleThreadedBottomCohort,
			StealPool oldPool, StealPool newPool);
}

