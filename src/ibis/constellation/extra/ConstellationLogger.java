package ibis.constellation.extra;

import ibis.constellation.ConstellationIdentifier;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class ConstellationLogger extends Logger {
    private static String FQCN = ConstellationLogger.class.getName() + ".";

    /**
     * Constructor.
     * 
     * Don't use directly. User getLogger(...) instead.
     */
    private ConstellationLogger(String name) {
	super(name);
    }

    public void fixme(Object message) {
	fixme(message, null);
    }

    public void fixme(Object message, Throwable t) {
	// TODO: For now we use the DEBUG level.
	// log4j documentation shows how to build a custom level.
	super.log(FQCN, Level.TRACE, "**** FIXME " + message + " FIXME ****", t);
    }

    public static ConstellationLogger getLogger(Class<?> clazz,
	    ConstellationIdentifier identifier) {
	return getLogger(clazz.getName(), identifier);
    }

    public static ConstellationLogger getLogger(String name,
	    ConstellationIdentifier identifier) {

	if (identifier == null) {
	    return (ConstellationLogger) Logger.getLogger(name);
	}

	return (ConstellationLogger) Logger.getLogger(name + '.' + identifier);
    }

}
