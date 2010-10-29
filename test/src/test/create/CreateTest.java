package test.create;

import ibis.constellation.Constellation;
import ibis.constellation.ConstellationFactory;
import ibis.constellation.SimpleExecutor;

public class CreateTest {

    public static void main(String [] args) { 
  
        // Simple test that creates, starts and stops a constellation
        try {
        	Constellation cohort = ConstellationFactory.createConstellation(new SimpleExecutor());
            cohort.activate();
            cohort.done();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
