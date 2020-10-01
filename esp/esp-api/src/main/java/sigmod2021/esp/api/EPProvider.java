package sigmod2021.esp.api;

import sigmod2021.common.EPException;
import sigmod2021.esp.api.epa.EPA;
import sigmod2021.esp.api.provider.*;
import sigmod2021.esp.api.util.StreamInfo;
import sigmod2021.event.EventSchema;

/**
 * An EPProvider offers all methods for creating, updating and removing
 * continuous queries.
 */
public interface EPProvider {

    /**
     * Retrieves metadata of the given stream
     *
     * @param name the stream's name
     * @return the associated meta information
     */
    StreamInfo getStreamInfo(String name) throws NoSuchStreamException;

    /**
     * Registers a new raw event-stream
     *
     * @param name   a unique name for the new stream
     * @param schema the schema of the new stream
     * @return a handle to access the newly registered stream
     * @throws AlreadyRegisteredException if a stream with the given name is already registered
     * @throws EPException                on any error registering the stream
     */
    StreamHandle registerStream(String name, EventSchema schema) throws AlreadyRegisteredException, EPException;

    /**
     * Registers a new external event-stream
     *
     * @param name   a unique name for the new stream
     * @param schema the schema of the new stream
     * @return a handle to access the newly registered stream
     * @throws AlreadyRegisteredException if a stream with the given name is already registered
     * @throws EPException                on any error registering the stream
     */
    ExternalStreamHandle registerExternalStream(String name, EventSchema schema) throws AlreadyRegisteredException, EPException;

    /**
     * Creates a new continuous query
     *
     * @param name the unique name of this query
     * @param epn  the EPA-graph representing this query
     * @return a handle to access the newly created query
     * @throws AlreadyRegisteredException if a query with the given name is already registered
     * @throws NoSuchStreamException      if this query consumes a raw stream which is not yet registered
     * @throws EPException                on any error creating the query
     */
    QueryHandle createQuery(String name, EPA epn) throws AlreadyRegisteredException, NoSuchStreamException, EPException;

    /**
     * Attaches the given sink to the given query
     *
     * @param query the handle referencing the target query
     * @param sink  the sink to attach
     * @throws NoSuchQueryException if the referenced query is not defined
     */
    void registerSink(QueryHandle query, EventSink sink) throws NoSuchQueryException;

    /**
     * Removes the given stream. All subsequent queries will also be removed!
     *
     * @param stream the stream to remove
     * @throws NoSuchStreamException if the referenced stream is not defined
     */
    void removeStream(DataSource stream) throws NoSuchStreamException;

    /**
     * Removes the given query
     *
     * @param query the query to remove
     * @throws NoSuchQueryException if the referenced query is not defined
     */
    void removeQuery(QueryHandle query) throws NoSuchQueryException;

    /**
     * Removes the given sink from the given query
     *
     * @param query the query to remove the sink from
     * @param sink  the sink to remove
     * @throws NoSuchQueryException if the referenced query is not defined
     */
    void removeSink(QueryHandle query, EventSink sink) throws NoSuchQueryException;

    /**
     * Removes the given sink from all queries it is attached to
     *
     * @param sink the sink to remove
     */
    void removeSink(EventSink sink);

    /**
     * Shuts down this provider. Any subsequent calls to any of this provider's methods will result
     * in undefined behaviour.
     *
     * @param flush determines whether all operators should flush their states (e.g., incomplete windows, output buffers)
     */
    void destroy(boolean flush);

    /**
     * Shuts down this provider. Any subsequent calls to any of this provider's methods will result
     * in undefined behaviour.
     */
    default void destroy() {
        destroy(false);
    }

}
