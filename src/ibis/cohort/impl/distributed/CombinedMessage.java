package ibis.cohort.impl.distributed;

import ibis.cohort.CohortIdentifier;

public class CombinedMessage extends Message {
    
    private static final long serialVersionUID = 6623230001381566142L;

    private Message [] messages;
    
    public CombinedMessage(CohortIdentifier source, CohortIdentifier target, 
            Message [] messages) { 
        super(source, target);
        this.messages = messages;
    }

    public Message [] getMessages() { 
        return messages;
    }
}
