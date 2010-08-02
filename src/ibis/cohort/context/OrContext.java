package ibis.cohort.context;

import java.util.ArrayList;

import ibis.cohort.Context;

public class OrContext extends Context {

    private static final long serialVersionUID = -1202476921345669674L;

    protected final UnitContext [] unitContexts;
    protected final AndContext [] andContexts; 
    
    protected final int hashCode;
    
    public OrContext(UnitContext [] unit, AndContext [] and) {        
        super();
      
        checkArrays(unit, and);
        
        if (unit == null) { 
            unit = new UnitContext[0];
        }
        
        if (and == null) { 
            and = new AndContext[0];
        }
        
        this.unitContexts = UnitContext.sort(unit); 
        this.andContexts = AndContext.sort(and);
    
        hashCode = 31*UnitContext.generateHash(unit) + AndContext.generateHash(and);
    }
    
    private void checkArrays(UnitContext [] unit, AndContext [] and) { 
        
        if (unit == null) {  
            if (and == null) { 
                throw new IllegalArgumentException("Invalid arguments to " +
                                "OrContext: 'unit' and 'and' cannot both be " +
                                "null!");
            } 
        
            if (and.length < 2) { 
                throw new IllegalArgumentException("Invalid arguments to " +
                                "OrContext: 'and' contains to few elements!");
            }
      
            return;
        } 
        
        if (and == null) {  
            
            if (unit.length < 2) { 
                throw new IllegalArgumentException("Invalid arguments to " +
                                "OrContext: 'unit' contains to few elements!");
            }
      
            return;
        }
        
        int len = unit.length + and.length;
        
        if (len < 2) { 
            throw new IllegalArgumentException("Invalid arguments to " +
                            "OrContext: 'unit+and' contains to few elements!");
        }
    }

    public OrContext(OrContext s) {        
        this(s.unitContexts, s.andContexts);
    }
    
    public boolean contains(UnitContext u) {
        
        for (int i=0;i<unitContexts.length;i++) { 
            if (u.equals(unitContexts[i])) { 
                return true;
            }
        }
        
        return false;
    }    

    public boolean contains(AndContext u) {

        for (int i=0;i<andContexts.length;i++) { 
            if (u.equals(andContexts[i])) { 
                return true;
            }
        }
        
        return false;
    }
    
    public boolean overlapping(OrContext other) {
        
        if (unitContexts.length != 0 && other.unitContexts.length != 0) {
            for (UnitContext u : other.unitContexts) { 
                if (contains(u)) { 
                    return true;
                }            
            }
        }

        if (andContexts.length != 0 && other.andContexts.length != 0) {
            for (AndContext a : other.andContexts) {
                if (contains(a)) { 
                    return true;
                }
            }
        } 
        
        return false;
    }

    public int countContexts() { 
        return andContexts.length + unitContexts.length;
    }
    
    public int countAndContexts() { 
        return andContexts.length;
    }
   
    public int countUnitContexts() { 
        return unitContexts.length;
    }
    
    public Context [] getContexts() { 
        
        int size = andContexts.length + unitContexts.length;
        
        Context [] tmp = new Context[size];
        
        System.arraycopy(unitContexts, 0, tmp, 0, unitContexts.length);
        System.arraycopy(andContexts, 0, tmp, unitContexts.length, andContexts.length);
        
        return tmp;
    }
  
    public UnitContext [] unitContexts() { 
        return unitContexts;
    }
    
    public AndContext [] andContexts() { 
        return andContexts;
    }
    
    @Override
    public boolean isOr() { 
        return true;
    }
    
    public boolean contains(Context other) {

        if (other.isUnit()) { 
            return contains((UnitContext)other);
        }

        if (other.isAnd()) { 
            return contains((AndContext)other);
        }

        if (other.isOr()) { 
            return contains((OrContext)other);
        }

        return false;
    }
   
    @Override
    public int hashCode() { 
        return hashCode;
    }
        
    @Override
    public boolean equals(Object obj) {

        // NOTE: potentially very expensive operations, especially with large 
        //       contexts that are equal
       
        if (this == obj) { 
            return true;
        }
        
        if (obj == null) { 
            return false;
        }
        
        if (getClass() != obj.getClass()) { 
            return false;
        }
        
        OrContext other = (OrContext) obj;
        
        if (hashCode != other.hashCode) { 
            return false;
        }
        
        if (unitContexts.length != other.unitContexts.length) { 
            return false;
        }

        if (andContexts.length != other.andContexts.length) { 
            return false;
        }

        for (int i=0;i<unitContexts.length;i++) { 
            if (!other.contains(unitContexts[i])) { 
                return false;
            }
        }
     
        for (int i=0;i<andContexts.length;i++) { 
            if (!other.contains(andContexts[i])) { 
                return false;
            }
        }
     
        return true;
    }    
    
    public String toString() { 
        
        StringBuilder b = new StringBuilder();
        
        b.append("ContextSet(");
        
        for (int i=0;i<unitContexts.length;i++) { 
            b.append(unitContexts[i]);
            
            if (i != unitContexts.length-1) { 
                b.append(" or ");
            }
        }
        
        for (int i=0;i<andContexts.length;i++) { 
            b.append(andContexts[i]);
            
            if (i != andContexts.length-1) { 
                b.append(" or ");
            }
        }
       
        b.append(")");
        
        return b.toString();
    }

    @Override
    public boolean satisfiedBy(Context other) {
        
        if (other == null) {
            return false;
        }
        
        if (other.isUnit()) { 
            return contains((UnitContext) other);
        }
        
        if (other.isAnd()) { 
            return contains((AndContext) other);
        }
    
        if (other.isOr()) { 
            return overlapping((OrContext) other);
        }
        
        return false;
    }

    public static Context merge(Context[] a) {

        if (a == null || a.length == 0) { 
            return UnitContext.DEFAULT;
        }
        
        ArrayList<UnitContext> unit = new ArrayList<UnitContext>();
        ArrayList<AndContext> and = new ArrayList<AndContext>();
     
        for (int i=0;i<a.length;i++) {
            Context tmp = a[i];
            
            if (tmp != null) { 

                // TODO: slow implementation!!!!
                if (tmp.isUnit()) {
                
                    if (!unit.contains(tmp)) { 
                        unit.add((UnitContext) tmp);
                    }
                } else if (tmp.isAnd()) { 
                    if (!and.contains(tmp)) { 
                        and.add((AndContext) tmp);
                    }
                } else if (tmp.isOr()) { 
                    
                    UnitContext [] uc = ((OrContext)tmp).unitContexts;
                    
                    for (int j=0;i<uc.length;j++) { 
                        
                        UnitContext u = uc[i];
                        
                        if (!unit.contains(u)) { 
                            unit.add(u);
                        }
                    }
                    
                    AndContext [] ac = ((OrContext)tmp).andContexts;
                    
                    for (int j=0;i<ac.length;j++) { 
                        
                        AndContext u = ac[i];
                        
                        if (!and.contains(u)) { 
                            and.add(u);
                        }
                    }
                }
            }
        }
  
        if (unit.size() == 0) { 
            
            int size = and.size();
            
            if (size == 0) { 
                return UnitContext.DEFAULT;
            }
        
            if (size == 1) { 
                return and.get(0);
            }
    
        } else if (unit.size() == 1) { 
            
            if (and.size() == 0) { 
                return unit.get(0);
            }
        }   
        
        return new OrContext(
                unit.toArray(new UnitContext[unit.size()]), 
                and.toArray(new AndContext[and.size()]));
    }
}
