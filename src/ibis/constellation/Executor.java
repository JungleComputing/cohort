package ibis.constellation;

import ibis.constellation.context.UnitWorkerContext;
import ibis.constellation.impl.ExecutorWrapper;

import java.io.Serializable;

public abstract class Executor implements Serializable {

	private static final long serialVersionUID = 6808516395963593310L;

	// NOTE: These are final for now... 
	private final WorkerContext context;
	
	private final StealStrategy localStealStrategy;
	private final StealStrategy remoteStealStrategy;
	
	private final StealPool myPool;
	private final StealPool stealsFrom;

	private ExecutorWrapper owner = null;

	protected Executor(StealPool myPool, StealPool stealsFrom, WorkerContext context, 
			StealStrategy localStealStrategy, StealStrategy remoteStealStrategy) { 

		if (myPool == null) { 
			this.myPool = StealPool.NONE;
		} else { 
			this.myPool = myPool;
		}

		if (stealsFrom == null) { 
			this.stealsFrom = StealPool.NONE;
		} else { 
			this.stealsFrom = stealsFrom;
		}

		if (context == null) { 
			this.context = UnitWorkerContext.DEFAULT;
		} else {
			this.context = context;
		}
		
		if (localStealStrategy == null) { 
			this.localStealStrategy = StealStrategy.ANY;
		} else { 
			this.localStealStrategy = localStealStrategy;
		}
		
		if (remoteStealStrategy == null) { 
			this.remoteStealStrategy = StealStrategy.ANY;
		} else { 
			this.remoteStealStrategy = remoteStealStrategy;
		}
	}

	protected Executor() { 
		this(StealPool.WORLD, StealPool.WORLD, UnitWorkerContext.DEFAULT, 
				StealStrategy.ANY, StealStrategy.ANY);
	}

	protected boolean processActivity() { 
		return false;
	}

	protected boolean processActivities() { 
		return owner.processActitivies();
	}

	public StealStrategy getLocalStealStrategy() { 
		return localStealStrategy;
	}

	public StealStrategy getRemoteStealStrategy() { 
		return remoteStealStrategy;
	}

	public ActivityIdentifier submit(Activity job) {
		return owner.submit(job);
	}

	public void send(Event e) {
		owner.send(e);
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

	public abstract void run();
}
