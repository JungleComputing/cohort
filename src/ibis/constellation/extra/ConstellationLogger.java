package ibis.constellation.extra;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;

import ibis.constellation.ConstellationIdentifier;


public class ConstellationLogger extends Logger {
    private static String FQCN = ConstellationLogger.class.getName() + ".";
    private static LoggerFactory factory = new ConstellationLoggerFactory();

    /**
     * Constructor.
     *
     * Don't use directly. User getLogger(...) instead.
     */
    public ConstellationLogger(String name) {
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

    public void info(Object message) {
        super.info(message);
    }

    /**
     * @deprecated Use warn(message)
     */
    @Deprecated
    public void warning(Object message) {
        warning(message, null);
    }

    /**
     * @deprecated Use warn(message, t)
     */
    @Deprecated
    public void warning(Object message, Throwable t) {
        super.log(FQCN, Level.WARN, message, t);
    }

    public static ConstellationLogger getLogger(Class clazz, ConstellationIdentifier identifier) {
        return getLogger(clazz.getName(), identifier);
    }

    public static ConstellationLogger getLogger(String name, ConstellationIdentifier identifier) {
        
        if (identifier == null) { 
            return (ConstellationLogger)Logger.getLogger(name, factory);
        }
        
        return (ConstellationLogger)Logger.getLogger(name + '.' + identifier, factory);
    }

}
