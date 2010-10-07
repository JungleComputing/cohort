package ibis.constellation.extra;

import java.io.PrintStream;

public class Log {
    
    private final String info;
    private final String warning;
    private final String fatal;
    private final String fixme;
    
    private final PrintStream out;
    
    private final boolean debug;
    
    public Log(String header, PrintStream out, boolean debug) { 
        this.out = out;
        this.debug = debug;
   
        this.info = "INFO " + header;
        this.warning = "WARNING " + header;
        this.fatal = "FATAL " + header;
        this.fixme = "**** FIXME " + header;
    }
    
    public void info(String text) { 
        if (debug) { 
            out.println(info + text);
        }
    }
    
    public void warning(String text) { 
        warning(text, null);
    }
     
    public void warning(String text, Exception e) { 
        out.println(warning + text);
        
        if (e != null) { 
            e.printStackTrace(out);
        }
        
        out.flush();
    }

    public void fixme(String text) { 
        fixme(text, null);
    }
     
    public void fixme(String text, Exception e) { 
        out.println(fixme + text + " FIXME ****");
        
        if (e != null) { 
            e.printStackTrace(out);
        }
        
        out.flush();
    }
    
    public void fatal(String text, Exception e) { 
        out.println(fatal + text);
        
        if (e != null) { 
            e.printStackTrace(out);
        }
        
        out.flush();
        System.exit(1);
    }

    public void flush() {
        out.flush(); 
    }


}
