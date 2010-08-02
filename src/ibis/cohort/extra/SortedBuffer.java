package ibis.cohort.extra;

import java.io.Serializable;
import java.util.Comparator;

public class SortedBuffer implements Serializable {

    private Object [] array;
    private int last = 0;
    
    private final Comparator comparator;
    
    public SortedBuffer(int initialSize, Comparator comparator) { 
        array = new Comparable[initialSize];
        this.comparator = comparator;
    }
    
    public int findIndex(Object o) { 
        
        // simple binary search
        int L = 1;
        int R = array.length+1;
        
        while (true) { 

            int p = (R-L) / 2;

            if (p == 0) { 
                return L-1;
            }

            p = p + L;

            int result = comparator.compare(array[p-1], o);

            if (result == 0) { 
                // We've hit the spot
                return p-1;
            } else if (result > 0) { 
                R = p;
            } else if (result < 0) { 
                L = p;
            }  
        }
    }
    
    private void resize(int size) { 
        Comparable [] tmp = new Comparable[size];
        System.arraycopy(array, 0, tmp, 0, array.length);
        array = tmp;
    }
    
    public void add(Object o) { 
       
        if (last == 0) { 
            array[0] = o;
            last++;
            return;
        }
        
        if (last == array.length-1) { 
            resize(array.length*2);
        }
        
        int index = findIndex(o);
        
        if (array[index] == null) { 
         
            
            
            
        }
        
    
    }
    
}
