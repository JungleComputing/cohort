package sor.rigid;

import java.io.Serializable;

public class Row implements Serializable {

    private static final long serialVersionUID = -1293856912078685925L;

    public final int iteration;
    public final int color;
    public final int rank;
    
    public final double [] data;
    
    public Row(int iteration, int color, int rank, double [] data) { 
        this.iteration = iteration;
        this.color = color;
        this.rank = rank;
        this.data = data;
    }
}
