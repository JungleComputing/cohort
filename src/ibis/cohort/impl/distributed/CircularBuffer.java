package ibis.cohort.impl.distributed;

import java.io.Serializable;

public class CircularBuffer implements Serializable {

    private static final long serialVersionUID = 5853279675709435595L;

    public Object [] array;

    public int first, next;
    public int size; 

    public CircularBuffer(int initialSize) {
        array = new Object[initialSize];
        first = 0;
        next = 0;
    }

    public boolean empty () {
        return (size == 0);
    } 

    public int size() {
        return size;
    } 
    
    public void insertFirst(Object item) {
        
        if (item == null) { 
            System.out.println("EEP: insertFirst null!!");
            new Exception().printStackTrace();
        }
        
        if (size >= array.length) { 
            resize();
        }

        if (first == 0) {
            first = array.length-1;
        } else { 
            first--;
        }
        
        array[first] = item;
        size++;
    }
    
    public void insertLast(Object item) {

        if (item == null) {
            System.out.println("EEP: insertLast null!!");
            new Exception().printStackTrace();
        }
        
        
        if (size >= array.length) { 
            resize();
        }

        array[next++] = item;
        size++;

        if (next >= array.length) { 
            next = 0;
        }
    }

    private void resize() {

        Object [] old = array;
        array = new Object[array.length*2];

        System.arraycopy(old, first, array, 0, old.length-first);
        System.arraycopy(old, 0, array, old.length-first, first);

        first = 0;
        next = old.length;      
    }

    public Object removeFirst() {

        if (size == 0) { 
            return null;
        }

        Object result = array[first];
        array[first] = null;
        first++;
        size--;

        if (first >= array.length) { 
            first = 0;
        }

        return result;
    } 

    public Object removeLast() {

        if (size == 0) { 
            return null;
        }

        if (next == 0) { 
            next = array.length-1;
        } else { 
            next--;
        }
       
        Object result = array[next];
        array[next] = null;
        size--;

        return result;
    } 

    public boolean remove(int index) { 

        if (index > size-1) { 
            return false;
        }
        
        if (index == 0) { 
            removeFirst();
        } else if (index == size) {
            removeLast();
        } else { 
            // TODO: optimize ? i.e., figure out how to move the least data
            int pos = (first + index) % array.length;  
            
            if (next > pos) { 
                // We simply move part of the data back
                while (pos < next-1) { 
                    array[pos] = array[pos+1];
                    pos++;
                }
                        
                next--;
                array[next] = null;
                size --;                
            } else { 
                // We simply move part of the data forward
                while (pos > first) { 
                    array[pos] = array[pos-1];
                    pos--;
                }
                        
                array[first] = null;
                first++;
                size --;                
            }            
        }
        
        return true;        
    }
    
    public boolean remove(Object o) {

        if (size == 0) { 
            return false;
        }
        
        int index = first;
        
        boolean removed = false;
        
        for (int i=0;i<size;i++) { 
            
            if (o.equals(array[index])) { 
                remove(i);
                removed = true;
            }
            
            index++;
        }
        
        return removed;        
    }
    
    public void clear() { 

        while (size > 0) { 
            array[first++] = null;
            size--;

            if (first >= array.length) { 
                first = 0;
            }
        }

        first = next = size = 0;                
    }
}
