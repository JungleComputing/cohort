package ibis.constellation.impl.distributed;

import ibis.constellation.ActivityIdentifierFactory;
import ibis.constellation.ConstellationIdentifier;
import ibis.constellation.StealPool;
import ibis.constellation.WorkerContext;
import ibis.constellation.extra.ConstellationIdentifierFactory;
import ibis.constellation.impl.distributed.single.SingleThreadedBottomConstellation;

public interface TopConstellation {
    /* 
     * This interface contains all methods a sub cohort will invoke on 
     * it's super cohort. 
     */
    
    /* synchronous methods - immediately produce a result or effect */ 
    void contextChanged(ConstellationIdentifier cid, WorkerContext newContext);
    
    ActivityIdentifierFactory getActivityIdentifierFactory(ConstellationIdentifier cid);
    ConstellationIdentifierFactory getCohortIdentifierFactory(ConstellationIdentifier cid);
    
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
    void register(BottomConstellation cohort) throws Exception;

	void registerPool(SingleThreadedBottomConstellation singleThreadedBottomCohort,
			StealPool oldPool, StealPool newPool);

	void registerStealPool(
			SingleThreadedBottomConstellation singleThreadedBottomCohort,
			StealPool oldPool, StealPool newPool);
}

