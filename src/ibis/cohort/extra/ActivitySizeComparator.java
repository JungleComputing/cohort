package ibis.cohort.extra;

import ibis.cohort.impl.distributed.ActivityRecord;

import java.util.Comparator;

public class ActivitySizeComparator implements Comparator {

    public int compare(Object a, Object b) {

        int ra = ((ActivityRecord) a).activity.getRank();
        int rb = ((ActivityRecord) b).activity.getRank();
       
        return ra - rb;
    }

}
