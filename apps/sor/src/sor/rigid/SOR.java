package sor.rigid;

import ibis.cohort.Activity;
import ibis.cohort.ActivityIdentifier;
import ibis.cohort.Context;
import ibis.cohort.Event;
import ibis.cohort.MessageEvent;
import ibis.cohort.context.UnitContext;
import ibis.cohort.extra.CircularBuffer;

public class SOR extends Activity { 

    // Termination criterion.
    private static final double TOLERANCE = 0.00001; 
    
    // The different states of the application.
    private static final int START         = 1;
    private static final int SEND          = 2;
    private static final int RECEIVE       = 3;
    private static final int COMPUTE_INNER = 4;
    private static final int COMPUTE_OUTER = 5;
    
    // The different phases of the computation.
    private static final int RED   = 0;
    private static final int BLACK = 1;
  
    // The activities we need to communicate with.
    private final ActivityIdentifier parent;
    private ActivityIdentifier prev;
    private ActivityIdentifier next;
    
    // Total number of activities and the rank of this activity.
    private final int size;
    private final int rank;
    
    // Problem size. 
    private final int N;
    
    // Required iterations and iterations performed
    private final int iterations;
    private int iterationCount;
   
    // Queues to temporarily queue data from other activities 
    private CircularBuffer prevRows;
    private CircularBuffer nextRows;
     
    // Current state and phase of the algorithm
    private int state = START; 
    private int color = RED;

    // The data
    private double[][] g;
    
    private double r;

    private double omega;

    private double stopdiff;

    private final int ncol;

    private final int nrow; /* number of rows and columns */

    private int lb;

    private int ub; /* lower and upper bound of grid stripe [lb ... ub] -> NOTE: ub is inclusive*/

    private double maxdiff = Double.MAX_VALUE;
    
    public SOR(ActivityIdentifier parent, int workers, int rank, int N, 
            int iterations) {

        super(new UnitContext("C-" + rank));

        this.parent = parent;
        this.size = workers;
        this.rank = rank;
        this.N = N;
        this.nrow = N;
        this.ncol = N;
        this.iterations = iterations;

        // NOTE: We do not initialize any data until the activity is started.
        //       This allows the activity to migrate to a different machine 
        //       before assuming it's true size.  
    }
    
    private void init() {
        
        nextRows = new CircularBuffer(2);
        prevRows = new CircularBuffer(2);
        
        // getBounds
        int n = N - 1;
        int nlarge = n % size;
        int size_small = n / size;
        int size_large = size_small + 1;

        int temp_lb;

        if (rank < nlarge) {
            temp_lb = rank * size_large;
            ub = temp_lb + size_large;
        } else {
            temp_lb = nlarge * size_large + (rank - nlarge) * size_small;
            ub = temp_lb + size_small;
        }

        if (temp_lb == 0) {
            lb = 1; /* row 0 is static */
        } else {
            lb = temp_lb;
        }
        // System.err.println(rank + ": my slice [" + lb + "," + ub + ">");

        r = 0.5 * (Math.cos(Math.PI / (ncol)) + Math.cos(Math.PI / (nrow)));
        double temp_omega = 2.0 / (1.0 + Math.sqrt(1.0 - r * r));
        stopdiff = TOLERANCE / (2.0 - temp_omega);
        omega = temp_omega * 0.8; /* magic factor */

        g = createGrid();

        initGrid();
        
        if (rank == 0) {
            System.out.println("Problem parameters");
            System.out.println("r       : " + r);
            System.out.println("omega   : " + omega);
            System.out.println("stopdiff: " + stopdiff);
            System.out.println("lb      : " + lb);
            System.out.println("ub      : " + ub);
            System.out.println("");
        }
    }
    
    private double[][] createGrid() {

        double[][] g = new double[nrow][];

        for (int i = lb - 1; i <= ub; i++) {
            // malloc the own range plus one more line
            // of overlap on each border
            g[i] = new double[ncol];
        }

        return g;
    }
    
    private void initGrid() {
        /* initialize the grid */
        for (int i = lb - 1; i <= ub; i++) {
            for (int j = 0; j < ncol; j++) {
                if (i == 0)
                    g[i][j] = 4.56;
                else if (i == nrow - 1)
                    g[i][j] = 9.85;
                else if (j == 0)
                    g[i][j] = 7.32;
                else if (j == ncol - 1)
                    g[i][j] = 6.88;
                else
                    g[i][j] = 0.0;
            }
        }
    }
    
    private double stencil(int row, int col) {
        return (g[row - 1][col] + g[row + 1][col] + g[row][col - 1] + g[row][col + 1]) / 4.0;
    }
 
    private double compute(int color, int lb, int ub) {
    
        double maxdiff = 0.0;

        for (int i = lb; i < ub; i++) {
            // int d = (even(i) ^ phase) ? 1 : 0;
            int d = (i + color) & 1;
            for (int j = 1 + d; j < ncol - 1; j += 2) {
                double gNew = stencil(i, j);
                double diff = Math.abs(gNew - g[i][j]);

                if (diff > maxdiff) {
                    maxdiff = diff;
                }

                g[i][j] += omega * (gNew - g[i][j]);
            }
        }

        return maxdiff;
    }
    
    @Override
    public void cancel() throws Exception {
        // Not implemented
    }

    @Override
    public void cleanup() throws Exception {
        cohort.send(identifier(), parent, rank);
    }

    @Override
    public void initialize() throws Exception {
        System.out.println("Cohort " + rank + " started!");      
        init();     // Just initialize the data and suspend...
        suspend();
    }

    private void send() {
        
        System.out.println(rank + ": send()");
        
        if (rank > 0) { 
            cohort.send(identifier(), prev, 
                    new Row(iterationCount, color, rank, g[lb]));
        } 

        if (rank < size-1) { 
            cohort.send(identifier(), next, 
                    new Row(iterationCount, color, rank, g[ub - 1]));
        }
    }
        
    private void receive(MessageEvent e) { 
      
        if (e.message instanceof Row) { 
            
            System.out.println(rank + ": Received row!");
            
            Row r = (Row) e.message;
            
            if (r.rank == rank+1) { 
                nextRows.insertLast(r);
            } else if (r.rank == rank-1) { 
                prevRows.insertLast(r);
            } else { 
                System.err.println("EEP: got Row message from wrong source! (" 
                        + rank + " -> " + r.rank + ")");
            }
            
            return;
        }
     
        if (e.message instanceof Neighbours) { 
     
            System.out.println(rank + ": Received neighbours");
            
            Neighbours n = (Neighbours) e.message;
            prev = n.previous;
            next = n.next;
            state = SEND;
            return;
        }
        
        System.err.println("EEP: got unknown event " + e); 
    } 
       
    private void insertBorders(Row prev, Row next) { 
        
        if (prev != null) { 
            
            // Sanity checks
            if (prev.color != color || prev.iteration != iterationCount) { 
                System.out.println("EEP: Row mismatch ? (" 
                        + color + " / " + prev.color + " " 
                        + iterationCount + " / " + prev.iteration);
            }
            
            g[lb - 1] = prev.data;
        } 
        
        if (next != null) { 
            
            // Sanity checks
            if (next.color != color || next.iteration != iterationCount) { 
                System.out.println("EEP: Row mismatch ? (" 
                        + color + " / " + next.color + " " 
                        + iterationCount + " / " + next.iteration);
            }
            
            g[ub] = next.data;
        }
    }
    
    @Override
    public void process(Event e) throws Exception {
  
        receive((MessageEvent) e);
 
        if (state == SEND) { 
            send();
            state = COMPUTE_INNER;
            compute(color, lb + 1, ub - 1);
            state = RECEIVE;
        } 
        
        if (state == RECEIVE) {
            
            if (rank == 0) { 
                if (nextRows.size() > 0) { 
                    insertBorders(null, (Row) nextRows.removeFirst());
                    state = COMPUTE_OUTER;
                } 
            } else if (rank > 0 && rank < size-1) {
                // I should have two waiting messages.
                if (prevRows.size() > 0 && nextRows.size() > 0) { 
                    insertBorders((Row)prevRows.removeFirst(), 
                                  (Row)nextRows.removeFirst());
                    state = COMPUTE_OUTER;
                } 
            } else { // rank == size-1
                if (prevRows.size() > 0) { 
                    insertBorders((Row)prevRows.removeFirst(), null);
                    state = COMPUTE_OUTER;
                }
            }
        } 
        
        if (state == COMPUTE_OUTER) { 
            compute(color, lb, lb + 1);
            compute(color, ub - 1, ub);
    
            if (color == RED) { 
                color = BLACK;
            } else { 
                color = RED;
                
                System.out.println("Finished iteration " + iterationCount);
                iterationCount++;
    
                if (iterationCount == iterations) { 
                    finish();
                    return;
                }
            }
     
            state = SEND;
        }
        
        if (state == SEND) { 
            send();
            state = COMPUTE_INNER;
            compute(color, lb + 1, ub - 1);
            state = RECEIVE;
        } 
        
        suspend();
    }
}
