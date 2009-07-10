package ibis.cohort;

public class MessageEvent<T> extends Event {

    public final T message;

    public MessageEvent(ActivityIdentifier source, ActivityIdentifier target, T message) {
        super(source, target);
        this.message = message;
    }

    public String toString() { 
        return "Message(" + source + " -> " + target + " : " + message + ")"; 
    }
    
}
