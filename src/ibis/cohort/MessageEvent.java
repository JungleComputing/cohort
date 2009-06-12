package ibis.cohort;

public class MessageEvent<T> extends Event {

    public final T message;

    public MessageEvent(Identifier source, T message) {
        super(source);
        this.message = message;
    }

}
