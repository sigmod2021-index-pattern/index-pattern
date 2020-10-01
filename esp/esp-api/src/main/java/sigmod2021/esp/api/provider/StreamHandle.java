package sigmod2021.esp.api.provider;

import sigmod2021.esp.api.EPProvider;

/**
 * Handle to a registered raw event stream. This is returned by the
 * active {@link EPProvider} after successful registration of a stream.
 * The handle may be used to push events into the engine or to remove
 * the stream.
 */
public interface StreamHandle extends DataSource {

    /**
     * Pushes the given event into the stream
     *
     * @param payload   the raw payload data
     * @param timestamp the timestamp of this event
     */
    void pushEvent(final Object[] payload, long timestamp);

}
