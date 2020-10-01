package sigmod2021.event.impl;

import sigmod2021.event.Event;

/**
 * Models an event build of 2 other events.
 * The temporal validity will be:
 * [ max(e1.getT1(), e2.getT1()), min(e1.getT2(), e2.getT2()) )
 */
public class CompoundEvent extends AbstractEvent {

    /**
     * The first event
     */
    private Event e1;

    /**
     * The second event
     */
    private Event e2;


    /**
     * Creates a new instance
     */
    public CompoundEvent() {
    }

    /**
     * Creates a new instance from the given events
     *
     * @param e1 the first event
     * @param e2 the second event
     */
    public CompoundEvent(Event e1, Event e2) {
        this.e1 = e1;
        this.e2 = e2;
    }

    /**
     * @return the first event
     */
    public Event getE1() {
        return e1;
    }

    /**
     * Sets the first event
     *
     * @param e1 the event to set
     */
    public void setE1(Event e1) {
        this.e1 = e1;
    }

    /**
     * @return the second event
     */
    public Event getE2() {
        return e2;
    }

    /**
     * Sets the second event
     *
     * @param e2 the event to set
     */
    public void setE2(Event e2) {
        this.e2 = e2;
    }

    /**
     * Materializes this compound event by copying
     * the contents of the member event into a new {@link SimpleEvent}
     *
     * @return the materialized version of this compound event
     */
    public SimpleEvent materialize() {
        Object[] payload = new Object[getNumberOfAttributes()];
        for (int i = 0; i < getNumberOfAttributes(); i++)
            payload[i] = get(i);
        return new SimpleEvent(payload, getT1(), getT2());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object get(int index) {
        if (index < e1.getNumberOfAttributes())
            return e1.get(index);
        else
            return e2.get(index - e1.getNumberOfAttributes());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberOfAttributes() {
        return e1.getNumberOfAttributes() + e2.getNumberOfAttributes();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T get(int index, Class<T> clazz) {
        return clazz.cast(get(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getT1() {
        return Math.max(e1.getT1(), e2.getT1());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getT2() {
        return Math.min(e1.getT2(), e2.getT2());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Event window(long t1, long t2) {
        return materialize().window(t1, t2);
    }
}
