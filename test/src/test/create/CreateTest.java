package test.create;

import ibis.constellation.Constellation;
import ibis.constellation.ConstellationFactory;
import ibis.constellation.SimpleExecutor;

public class CreateTest {

    public static void main(String [] args) { 
  
        // Simple test that creates, starts and stops a cohort
        try {
        	Constellation cohort = ConstellationFactory.createCohort(new SimpleExecutor());
            cohort.activate();
            cohort.done();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
