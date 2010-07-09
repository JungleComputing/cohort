package test.create;

import ibis.cohort.Cohort;
import ibis.cohort.CohortFactory;

public class CreateTest {

    public static void main(String [] args) { 
  
        // Simple test that creates, starts and stops a cohort
        try {
            Cohort cohort = CohortFactory.createCohort();
            cohort.activate();
            cohort.done();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
