package test.create;

import ibis.cohort.Cohort;
import ibis.cohort.CohortFactory;
import ibis.cohort.SimpleExecutor;

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
