package ibis.constellation.impl;

import ibis.constellation.ConstellationIdentifier;

public class StealReply extends Message {
    
    private static final long serialVersionUID = 2655647847327367590L;
   
    private final ActivityRecord [] work;
    
    public StealReply(
            final ConstellationIdentifier source, 
            final ConstellationIdentifier target, 
            final ActivityRecord work) { 
        super(source, target);
        
        if (work == null) { 
            this.work = null;
        } else { 
            this.work = new ActivityRecord [] {work};
        }
    }
    
    public StealReply(
            final ConstellationIdentifier source, 
            final ConstellationIdentifier target, 
            final ActivityRecord [] work) { 
        super(source, target);
        this.work = work;
    }
    
    public boolean isEmpty() { 
        return (work == null || work.length == 0);
    }
    
    public ActivityRecord [] getWork() { 
        return work;
    }
    
    public ActivityRecord getWork(int i) { 
        return work[i];
    }
    
    public int getSize() {
        
        if (work == null) { 
            return 0;
        }
        
        // Note: assumes array is filled!
        return work.length;
    }
}
