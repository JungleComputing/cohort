package ibis.cohort.extra;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;

import ibis.cohort.CohortIdentifier;


public class CohortLogger extends Logger {
    private static String FQCN = CohortLogger.class.getName() + ".";
    private static LoggerFactory factory = new CohortLoggerFactory();

    /**
     * Constructor.
     *
     * Don't use directly. User getLogger(...) instead.
     */
    public CohortLogger(String name) {
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

    public static CohortLogger getLogger(Class clazz, CohortIdentifier identifier) {
        return getLogger(clazz.getName(), identifier);
    }

    public static CohortLogger getLogger(String name, CohortIdentifier identifier) {
        return (CohortLogger)Logger.getLogger(name + '.' + identifier, factory);
    }

}
