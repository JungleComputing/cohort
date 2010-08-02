package ibis.cohort;

public class CancelEvent extends Event {

    private static final long serialVersionUID = 868969038833374231L;

    public CancelEvent(ActivityIdentifier target) {
        super(null, target);
    }
}
