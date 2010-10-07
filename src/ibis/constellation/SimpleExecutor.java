package ibis.constellation;

import ibis.constellation.context.UnitWorkerContext;

public class SimpleExecutor extends Executor {

	private static final long serialVersionUID = -2498570099898761363L;

	public SimpleExecutor(StealPool pool, StealPool stealFrom, WorkerContext c) { 
		super(pool, stealFrom, c);
	}
	
	public SimpleExecutor() { 
		this(StealPool.WORLD, StealPool.WORLD, UnitWorkerContext.DEFAULT);
	}
	
	public SimpleExecutor(WorkerContext wc) { 
		this(StealPool.WORLD, StealPool.WORLD, wc);
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
