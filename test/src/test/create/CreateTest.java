package test.create;

import ibis.constellation.Cohort;
import ibis.constellation.CohortFactory;
import ibis.constellation.SimpleExecutor;

public class CreateTest {

    public static void main(String [] args) { 
  
        // Simple test that creates, starts and stops a cohort
        try {
        	Cohort cohort = CohortFactory.createCohort(new SimpleExecutor());
            cohort.activate();
            cohort.done();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
