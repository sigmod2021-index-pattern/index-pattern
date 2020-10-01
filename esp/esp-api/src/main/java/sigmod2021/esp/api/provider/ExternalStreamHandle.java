package sigmod2021.esp.api.provider;

import sigmod2021.esp.api.EPProvider;
import sigmod2021.event.Event;

/**
 * Handle to an external event stream. This is returned by the
 * active {@link EPProvider} after successful registration of a stream.
 * The handle may be used to push events into the engine or to remove
 * the stream.
 */
public interface ExternalStreamHandle extends DataSource {

    /**
     * Pushes the given interval-event into the system
     *
     * @param event
     */
    void pushEvent(Event event);

}
