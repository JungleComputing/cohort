package genesequencing;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Context;
import ibis.cohort.Event;
import ibis.cohort.MessageEvent;

import java.util.ArrayList;

public class DivCon extends Activity {

    private final ActivityIdentifier parent;
    private final WorkUnit workUnit; 
    private final int myIndex; 
    
    private ArrayList<ResSeq> result;
    private ArrayList<ResSeq> [] sub;    
    private int count;
    
    public DivCon(ActivityIdentifier parent, WorkUnit workUnit, int myIndex) {
        super(Context.ANYWHERE);
        this.parent = parent;
        this.workUnit = workUnit;
        this.myIndex = myIndex;
    }

    @Override
    public void initialize() throws Exception {

        // We first split the problem into 2 subproblems until it is small 
        // enough to solve trivially.  

        int querySize = workUnit.querySequences.size();
        int databaseSize = workUnit.databaseSequences.size();
        int size = querySize > databaseSize ? querySize : databaseSize;
        
        if (size <= workUnit.threshold) {
            // Trivial case
            result = Dsearch.createTrivialResult(workUnit);
            finish();
        } else {
            // Split case. Note that the order of the results is important, so 
            // we pass an 'index' to each subjob.

            if (databaseSize <= workUnit.threshold) {
                // Only split queries if the database is small enough. 
                
                int newSplitSize = querySize / 2;
                
                cohort.submit(new DivCon(identifier(), 
                        workUnit.splitQuerySequences(0, newSplitSize), 0));
                
                cohort.submit(new DivCon(identifier(), 
                        workUnit.splitQuerySequences(newSplitSize, querySize), 1));
                                              
            } else {
                
                // If the database is large we split it first.                 
                int newSplitSize = databaseSize / 2;
                
                cohort.submit(new DivCon(identifier(),
                        workUnit.splitDatabaseSequences(0, newSplitSize), 0));
                
                cohort.submit(new DivCon(identifier(),
                        workUnit.splitDatabaseSequences(newSplitSize, databaseSize), 1));
            }
            
            suspend();
        }
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void process(Event e) throws Exception {
        
        // Receive the sub results, an merge them once they are all in. 
        MessageEvent tmp = (MessageEvent) e;
        
        if (sub == null) {
            sub = (ArrayList<ResSeq>[])(new ArrayList[2]);            
        }
        
        Result res = (Result) tmp.message;
        
        sub[res.index] = res.result;  

        count++;
        
        if (count == 2) {            
            result = Dsearch.combineSubResults(sub);            
            finish();
        } else { 
            suspend();
        }        
    }

    @Override
    public void cleanup() throws Exception {
        // Send the result to our parent
        cohort.send(identifier(), parent, new Result(myIndex, result));
    }
}
