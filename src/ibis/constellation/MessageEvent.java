package ibis.constellation;

public class MessageEvent extends Event {

    private static final long serialVersionUID = -1008257241415877144L;
    
    public final Object message;

    public MessageEvent(ActivityIdentifier source, ActivityIdentifier target, Object message) {
        super(source, target);
        this.message = message;
    }

    public String toString() { 
        return "Message(" + source + " -> " + target + " : " + message + ")"; 
    }
    
}
