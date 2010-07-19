package ibis.cohort.extra;

import ibis.cohort.Context;
import ibis.cohort.impl.distributed.ActivityRecord;

public class OptimizedSimpleWorkQueue extends SimpleWorkQueue {
    
    // Slightly optimized version of simple workqueue which adds an optimized 
    // version of steal(Context, count).
    
    @Override
    public ActivityRecord [] steal(Context c, int count) {
     
        if (buffer.empty()) { 
            return null;
        }
        
        ActivityRecord [] result = new ActivityRecord[count];
        
        if (c.isAny()) { 
            for (int i=0;i<count;i++) { 
                result[i] = (ActivityRecord) buffer.removeFirst();
         
                if (buffer.empty()) { 
                    return trim(result, i);
                }
            }
   
            return result;
        }
     
        int size = 0;
        int index = 0;
        
        while (index < buffer.size()) { 
            
            Context tmp = ((ActivityRecord) buffer.get(index)).activity.getContext();
            
            if (tmp.contains(c)) { 
                result[size++] = (ActivityRecord) buffer.get(index);
                buffer.remove(index);
                size++;
        
                // NOTE: we do NOT increment index, as we have just removed an 
                //    entry. Therefore, there is a new entry on position index! 
            } else { 
                index++;
            }
        }
        
        if (size < count) { 
            return trim(result, size);
        } else { 
            return result;
        }
    }
}
