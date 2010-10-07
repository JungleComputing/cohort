package ibis.constellation;

public class MessageEvent<T> extends Event {

    private static final long serialVersionUID = -1008257241415877144L;
    
    public final T message;

    public MessageEvent(ActivityIdentifier source, ActivityIdentifier target, T message) {
        super(source, target);
        this.message = message;
    }

    public String toString() { 
        return "Message(" + source + " -> " + target + " : " + message + ")"; 
    }
    
}
