package genesequencing;

import java.io.Serializable;
import java.util.ArrayList;

class Result implements Serializable {
    
    private static final long serialVersionUID = -9080204456880183670L;
    
    public final int index;
    public final ArrayList<ResSeq> result;
    
    Result(int index, ArrayList<ResSeq> result) { 
        this.index = index;
        this.result = result;
    }        
}