package sigmod2021.esp.api.bridge;

import sigmod2021.event.Event;
import sigmod2021.event.EventSchema;

/**
 * Represents an event stream. This is either a stream
 * supplied with external events or the output-stream
 * of an EPA.
 */
public interface EventChannel {

    /**
     * @return the schema of this event stream
     */
    EventSchema getSchema();

    /**
     * Pushes the given event into this stream
     *
     * @param event the event to push
     */
    void push(Event event);

}
