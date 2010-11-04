package test.spawntest;

import ibis.constellation.Constellation;
import ibis.constellation.ConstellationFactory;
import ibis.constellation.Executor;
import ibis.constellation.SimpleExecutor;
import ibis.constellation.SingleEventCollector;
import ibis.constellation.StealStrategy;
import ibis.constellation.context.UnitWorkerContext;

public class SpawnTest {

	// Cleaner version of the spawntest...

	private static final int SPAWNS_PER_SYNC = 10;
	private static final int COUNT = 1000000;    
	private static final int REPEAT = 10;

	public static void main(String [] args) { 

		try { 
			Constellation c = ConstellationFactory.createConstellation(
					new SimpleExecutor(new UnitWorkerContext("DEFAULT"),  StealStrategy.SMALLEST));

			c.activate();

			for (int i=0;i<REPEAT;i++) { 
				SingleEventCollector a = new SingleEventCollector();
				c.submit(a);
				c.submit(new TestLoop(a.identifier(), COUNT, SPAWNS_PER_SYNC));
				a.waitForEvent();
			}

			c.done();

		} catch (Exception e) {
			System.err.println("Oops: " + e);
			e.printStackTrace(System.err);
			System.exit(1);
		}
	}



}
