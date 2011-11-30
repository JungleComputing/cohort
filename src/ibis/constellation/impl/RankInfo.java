package ibis.constellation.impl;

import java.io.Serializable;

import ibis.ipl.IbisIdentifier;

public class RankInfo implements Serializable {

    private static final long serialVersionUID = 7620973089142583450L;
   
    public int rank; 
    public IbisIdentifier id;
    
    public RankInfo(int rank, IbisIdentifier id) {
        super();
        this.rank = rank;
        this.id = id;
    }
}
