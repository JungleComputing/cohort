package ibis.cohort;

import java.io.Serializable;

public class Location implements Serializable {

	private static final long serialVersionUID = -2442900962246421740L;

	public final static Location HERE       = new Location("here");
	public final static Location EVERYWHERE = new Location("everywhere");
	public final static Location ANYWHERE   = new Location("anywhere");
	
	public final String name; 
	
	public Location(String name) { 
		this.name = name;
	}
}
