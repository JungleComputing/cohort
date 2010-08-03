package ibis.cohort.extra;

import java.util.Comparator;

public class SortedList {

    static class Node { 
        Node next;
        Node prev;
        
        final Object data;
        
        Node(Object data) { 
            this.data = data;
        }
    }

    private final Comparator comparator;
    
    private Node head;
    private Node tail;
    private int size;
    
    public SortedList(Comparator comparator) { 
        this.comparator = comparator;
        head = tail = null;
        size = 0;
    }
          
    public void insert(Object o) { 
  
        Node n = new Node(o);
        
        // Check if the list is empty
        if (size == 0) { 
            head = tail = n;
            size = 1;
            return;
        }
      
        // Check if the list contains a single element
        if (size == 1) { 
           
            int tmp = comparator.compare(o, head.data);
            
            if (tmp <= 0) { 
                n.next = head;
                head.prev = n;
                head = n;
            } else { 
                n.prev = tail;
                tail.next = n;
                tail = n;
            }
            
            size++;
            return;
        }
        
        // Check if the new element goes before/at the head
        int tmp = comparator.compare(o, head.data);
        
        if (tmp <= 0) { 
            n.next = head;
            head.prev = n;
            head = n;
            size++;
            return;
        }
       
        // Check if the new element goes at/after the tail
        tmp = comparator.compare(o, tail.data);
        
        if (tmp >= 0) { 
            n.prev = tail;
            tail.next = n;
            tail = n;
            size++;
            return;
        }
        
        Node current = head.next;
        
        while (current != null) { 
           
            // Check if the new element goes at/before the current
            tmp = comparator.compare(o, current.data);
                
            if (tmp <= 0) { 
            
                n.prev = current.prev;
                current.prev.next = n;
                
                n.next = current;
                current.prev = n;
          
                size++;
                return;
            }
        }
        
        // When we run out of nodes we insert at the end -- SHOULD NOT HAPPEN!--
        System.err.println("EEEP: sorted list screwed up!!!");
        
        n.prev = tail;
        tail.next = n;
        tail = n;
        size++;
    }
    
    public Object removeHead() { 
     
        if (size == 0) { 
            return null;
        }
        
        Object tmp = head.data;
        
        if (size == 1) { 
            head = tail = null;
            size = 0;
            return tmp;
        }
        
        head = head.next;
        head.prev = null;
        size--;
        
        return tmp;
    }
    
    public Object removeTail() { 
     
        if (size == 0) { 
            return null;
        }
        
        Object tmp = tail.data;
        
        if (size == 1) { 
            head = tail = null;
            size = 0;
            return tmp;
        }
        
        tail = tail.prev;
        tail.next = null;
        size--;
        
        return tmp;
    }   

    public int size() { 
        return size;
    }

    public boolean removeByReference(Object o) {
    
        Node current = head;
        
        while (current != null) { 
            
            if (current.data == o) { 
        
                // Found it
                if (size == 1) { 
                    head = tail = null;
                    size = 0;
                    return true;
                }
            
                if (current == head) { 
                    head = head.next;
                    head.prev = null;
                    size--;
                    return true;
                }
                
                if (current == tail) { 
                    tail = tail.prev;
                    tail.next = null;
                    size--;
                    return true;
                }
                
                current.prev.next = current.next;
                current.next.prev = current.prev;
                current.prev = null;
                current.next = null;
                size--;
                return true;
            }
       
            current = current.next;
        }
        
        return false;
    }
}
