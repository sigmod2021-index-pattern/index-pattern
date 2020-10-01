package sigmod2021.event.impl;

/**
 * Models a simple event holding the payload in
 * an array of objects.
 */
public class SimpleEvent extends AbstractEvent {


    /**
     * The payload
     */
    protected final Object[] payload;

    /**
     * the start of this event's temporal validity (inclusive)
     */
    private final long t1;

    /**
     * the end of this event's temporal validity (exclusive)
     */
    private final long t2;

    /**
     * Creates a new instance
     *
     * @param payload the event's payload
     * @param t1      the start of this event's temporal validity (inclusive)
     * @param t2      the end of this event's temporal validity (exclusive)
     */
    public SimpleEvent(Object[] payload, long t1, long t2) {
        this.payload = payload;
        this.t1 = t1;
        this.t2 = t2;
    }

    /**
     * Constructor for chronon events
     *
     * @param payload
     * @param timestamp
     */
    public SimpleEvent(Object[] payload, long timestamp) {
        this.payload = payload;
        this.t1 = timestamp;
        this.t2 = timestamp + 1;
    }

    /**
     * {@inheritDoc}
     */
    public SimpleEvent window(long t1, long t2) {
        return new SimpleEvent(payload, t1, t2);
    }

    /**
     * {@inheritDoc}
     */
    public Object get(int index) {
        return payload[index];
    }

    /**
     * {@inheritDoc}
     */
    public <T> T get(int index, Class<T> clazz) {
        return clazz.cast(payload[index]);
    }

    /**
     * {@inheritDoc}
     */
    public long getT1() {
        return t1;
    }

    /**
     * {@inheritDoc}
     */
    public long getT2() {
        return t2;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberOfAttributes() {
        return payload.length;
    }

    public Object[] getPayload() {
        return payload;
    }
}
