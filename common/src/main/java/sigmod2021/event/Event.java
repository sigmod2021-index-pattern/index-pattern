package sigmod2021.event;

/**
 * Represents a single event including its
 * interval of temporal validity.
 */
public interface Event {

    /**
     * Retrieves the value for the index-th field of this event
     *
     * @param index the index
     * @return the value stored at index
     */
    Object get(int index);

    /**
     * Retrieves the value for the index-th field of this event
     * and casts it to the supplied type.
     *
     * @param index the index
     * @return the value stored at index
     */
    <T> T get(int index, Class<T> clazz);

    /**
     * @return the start of this event's temporal validity (inclusive)
     */
    long getT1();

    /**
     * @return the end of this event's temporal validity (exclusive)
     */
    long getT2();

    /**
     * @return the number of attributes stored in this event
     */
    int getNumberOfAttributes();

    /**
     * Creates a new event with the same payload, but different temporal validity
     *
     * @param t1 the new start of this event's temporal validity (inclusive)
     * @param t2 the new end of this event's temporal validity (inclusive)
     * @return the resulting event
     */
    Event window(long t1, long t2);
}
