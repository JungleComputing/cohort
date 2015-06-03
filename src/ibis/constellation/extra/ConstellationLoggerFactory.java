package ibis.constellation.extra;

import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;


public class ConstellationLoggerFactory implements LoggerFactory {
    
    public Logger makeNewLoggerInstance(String name) {
        return new ConstellationLogger(name);
    }
}
