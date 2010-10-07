package ibis.constellation;

import ibis.constellation.context.UnitWorkerContext;
import ibis.constellation.impl.single.ExecutorWrapper;

import java.io.Serializable;

public abstract class Executor implements Serializable {

	private static final long serialVersionUID = 6808516395963593310L;

	// NOTE: These are final for now... 
	private final WorkerContext context;
	private final StealPool myPool;
	private final StealPool stealsFrom;

	private ExecutorWrapper owner = null;

	protected Executor(StealPool myPool, StealPool stealsFrom, WorkerContext context) { 
		this.myPool = myPool;
		this.stealsFrom = stealsFrom;
		this.context = context;
	}
	
	protected Executor() { 
		this(StealPool.WORLD, StealPool.WORLD, UnitWorkerContext.DEFAULT);
	}
	
	public WorkerContext getContext() { 
		return context;
	}
	
	public synchronized void connect(ExecutorWrapper owner) throws Exception {
		
		if (this.owner != null) { 
			throw new Exception("Executor already connected!");
		}
		
		this.owner = owner;		
	}
	
	public StealPool belongsTo() {
		return myPool;
	}
	
	public StealPool stealsFrom() {
		return stealsFrom;
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
