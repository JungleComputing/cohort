package ibis.constellation;

import ibis.constellation.context.UnitWorkerContext;

public class SimpleExecutor extends Executor {

	private static final long serialVersionUID = -2498570099898761363L;

	public SimpleExecutor(StealPool pool, StealPool stealFrom, WorkerContext c, StealStrategy local, StealStrategy remote) { 
		super(pool, stealFrom, c, local, remote);
	}
	
	public SimpleExecutor() { 
		super(StealPool.WORLD, StealPool.WORLD, UnitWorkerContext.DEFAULT, StealStrategy.ANY, StealStrategy.ANY);
	}
	
	public SimpleExecutor(WorkerContext wc) { 
		super(StealPool.WORLD, StealPool.WORLD, wc, StealStrategy.ANY, StealStrategy.ANY);
	}
	
	public SimpleExecutor(WorkerContext wc, StealStrategy s) { 
		super(StealPool.WORLD, StealPool.WORLD, wc, s, s);
	}
	
	public SimpleExecutor(WorkerContext wc, StealStrategy local, StealStrategy remote) { 
		super(StealPool.WORLD, StealPool.WORLD, wc, local, remote);
	}
	
	@Override
	public void run() {
		
		System.out.println("Starting Executor!");
		
		boolean done = false;
		
		while (!done) { 
			done = processActivities();
		}
		
		System.out.println("Executor done!");
		
	}

}
