package ibis.cohort.impl.multithreaded;

public class CircularBuffer {

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
