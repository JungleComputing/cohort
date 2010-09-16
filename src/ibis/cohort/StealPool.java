package ibis.cohort;

import java.io.Serializable;
import java.util.HashSet;

public class StealPool implements Serializable {
	
	private static final long serialVersionUID = 2379118089625564822L;

	public static StealPool WORLD = new StealPool("WORLD");
	public static StealPool NONE = new StealPool("NONE");
	
	private final String tag;
	private final boolean isSet;
	private final StealPool [] set;
	
	public StealPool(StealPool ... set) { 
		
		if (set == null || set.length == 0) { 
			throw new IllegalArgumentException("StealPool set cannot be empty!");
		}
		
		HashSet<StealPool> tmp = new HashSet<StealPool>();

		for (int i=0;i<set.length;i++) { 
			if (set[i] == null) { 
				throw new IllegalArgumentException("StealPool set cannot be sparse!");
			}
			
			if (set[i].isSet) { 
				throw new IllegalArgumentException("StealPool cannot be recursive!");
			}

			tmp.add(set[i]);
		}
		
		tag = null;
		isSet = true;
		this.set = tmp.toArray(new StealPool[tmp.size()]);
	}
	
	public StealPool(String tag) { 
		this.tag = tag;
		isSet = false;
		set = null;
	}
	
	public String getTag() { 
		return tag;
	}

	public boolean isSet() { 
		return isSet;
	}
	
	public StealPool [] set() { 
		return set;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((tag == null) ? 0 : tag.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StealPool other = (StealPool) obj;
		if (tag == null) {
			if (other.tag != null)
				return false;
		} else if (!tag.equals(other.tag))
			return false;
		return true;
	}
	
	public static StealPool merge(StealPool ... pools) { 

		if (pools == null || pools.length == 0) { 
			throw new IllegalArgumentException("StealPool list cannot be empty!");
		}
		
		HashSet<StealPool> tmp = new HashSet<StealPool>();

		for (int i=0;i<pools.length;i++) { 
			
			StealPool s = pools[i];
			
			if (s == null) { 
				throw new IllegalArgumentException("StealPool list cannot be sparse!");
			}
			
			if (s.isSet) { 
			
				StealPool [] s2 = s.set();
				
				for (int j=0;j<s2.length;j++) { 
					tmp.add(s2[i]);
				}
			} else { 
				tmp.add(s);
			}
		}
	
		if (tmp.size() == 0) { 
			throw new IllegalArgumentException("StealPool list cannot be empty!");
		}

		if (tmp.size() == 1) { 
			return tmp.iterator().next();
		}
		
		return new StealPool(tmp.toArray(new StealPool[tmp.size()]));
	}	
}
