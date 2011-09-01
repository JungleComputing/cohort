package ibis.constellation.impl;

import ibis.constellation.ConstellationIdentifier;
import ibis.constellation.StealPool;
import ibis.constellation.WorkerContext;

public class StealReply extends Message {

    private static final long serialVersionUID = 2655647847327367590L;

    private final StealPool pool;
    private final WorkerContext context;
    private final ActivityRecord [] work;

    public StealReply(
            final ConstellationIdentifier source,
            final ConstellationIdentifier target,
            final StealPool pool,
            final WorkerContext context,
            final ActivityRecord work) {

        super(source, target);

        if (work == null) {
            this.work = null;
        } else {
            this.work = new ActivityRecord [] {work};
        }
        this.pool = pool;
        this.context = context;
    }

    public StealReply(
            final ConstellationIdentifier source,
            final ConstellationIdentifier target,
            final StealPool pool,
            final WorkerContext context,
            final ActivityRecord [] work) {
        super(source, target);

        if (work == null || work.length == 0) {
            throw new IllegalArgumentException("Work may not be empty!");
        }

        this.pool = pool;
        this.work = work;
        this.context = context;
    }

    public boolean isEmpty() {
        return (work == null || work.length == 0);
    }

    public StealPool getPool() {
        return pool;
    }

    public WorkerContext getContex() {
        return context;
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
