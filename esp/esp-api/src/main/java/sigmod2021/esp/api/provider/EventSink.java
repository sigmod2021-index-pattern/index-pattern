package sigmod2021.esp.api.provider;

import sigmod2021.event.Event;

/**
 * A sink may be attached to a running query and receives
 * all results produced by it. It is typically used to trigger actions
 * based on the received results.
 */
public interface EventSink {

    default boolean convertGeometries() {
        return true;
    }

    /**
     * Invoked on every event leaving the query this sink is attached to.
     *
     * @param event the query-result
     */
    void receiveEvent(final Event event);

}
