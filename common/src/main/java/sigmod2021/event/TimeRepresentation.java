package sigmod2021.event;

/**
 * Available time representation in streams.
 */
public enum TimeRepresentation {

    /**
     * Represents events with time intervals, indicating the time the event is valid
     */
    INTERVAL,

    /**
     * Represents events with validity during a single point in time
     */
    POINT,

    /**
     * Represents events without time stamps. Therefore, the current time is used.
     */
    NONE;

}
