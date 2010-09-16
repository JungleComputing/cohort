package ibis.cohort;

import ibis.cohort.context.UnitWorkerContext;
import ibis.cohort.impl.distributed.single.ExecutorWrapper;

import java.io.Serializable;

public abstract class Executor implements Serializable {

	private static final long serialVersionUID = 6808516395963593310L;

	private ExecutorWrapper owner = null;
	
	private WorkerContext context;
	
	private StealPool myPool;
	private StealPool stealsFrom;

	private boolean poolIsFixed;
	private boolean stealIsFixed;

	protected Executor(StealPool myPool, boolean poolFixed, StealPool stealsFrom, boolean stealFixed, WorkerContext context) { 
		this.myPool = myPool;
		this.poolIsFixed = poolFixed;
		this.stealsFrom = stealsFrom;
		this.stealIsFixed = stealFixed;
		this.context = context;
	}
	
	protected Executor() { 
		this(StealPool.WORLD, false, StealPool.WORLD, false, UnitWorkerContext.DEFAULT);
	}

	protected synchronized void setContext(WorkerContext c) { 
		context = c;
		owner.registerContext(this, c);
	}
	
	protected synchronized WorkerContext getContext() { 
		return context;
	}
	
	public synchronized void connect(ExecutorWrapper owner) throws Exception {
		
		if (this.owner != null) { 
			throw new Exception("Executor already connected!");
		}
		
		this.owner = owner;		
		owner.registerPool(this, myPool, poolIsFixed);
		owner.registerStealPool(this, stealsFrom, stealIsFixed);
		owner.registerContext(this, getContext());
	}
	
	protected synchronized void belongsTo(StealPool pool, boolean fixed) throws IllegalArgumentException {
		
		if (this.poolIsFixed) { 
			throw new IllegalArgumentException("StealPool is fixed and cannot be changed!");
		}
		
		myPool = pool;
		poolIsFixed = fixed;
	
		if (owner != null) { 
			owner.registerPool(this, myPool, poolIsFixed);
		}
	}
	
	protected void belongsTo(StealPool pool) {
		belongsTo(pool, false);
	}

	protected synchronized void stealsFrom(StealPool pool, boolean fixed) {
		
		if (this.stealIsFixed) { 
			throw new IllegalArgumentException("StealPool is fixed and cannot be changed!");
		}
		
		this.stealsFrom = pool;
		this.stealIsFixed = fixed;
		
		if (owner != null) { 
			owner.registerStealPool(this, stealsFrom, stealIsFixed);
		}
	}
	
	protected void stealsFrom(StealPool pool) {
		stealsFrom(pool, false);
	}
	
	protected boolean processActivity() { 
		return false;
	}
	
	protected boolean processActivities() { 
		return owner.processActitivies();
	}

	public ActivityIdentifier submit(Activity job) {
		return owner.submit(job);
	}

	public void send(Event e) {
		owner.send(e);
	}
	
	public void send(ActivityIdentifier source, ActivityIdentifier target, Object o) { 
		owner.send(source, target, o);
	}
	
	public abstract void run();
	
	
	
}
