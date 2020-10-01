package sigmod2021.esp.api.bridge;

import sigmod2021.common.EPException;
import sigmod2021.esp.api.EPProvider;
import sigmod2021.esp.api.epa.*;
import sigmod2021.esp.api.epa.window.Window;
import sigmod2021.esp.api.provider.NoSuchQueryException;
import sigmod2021.esp.api.provider.NoSuchStreamException;
import sigmod2021.event.EventSchema;

/**
 * Interface to a specific EPProvider
 * Bridges must NOT forward results of EPAs directly, but
 * pass them to the supplied callback. The active {@link EPProvider} will
 * feed them back into their resulting {@link EventChannel}
 */
public interface EPBridge {

    /**
     * Registers a new event-stream
     *
     * @param schema the schema of the stream to register
     * @return a reference to the registered stream
     * @throws EPException on any error registering the stream
     */
    EventChannel registerStream(EventSchema schema) throws EPException;

    /**
     * Deploys a new window EPA based on the given definition
     *
     * @param window   the EPA definition
     * @param callback the callback to invoke for events leaving this EPA
     * @param input    the input-stream
     * @return a reference to the window's result-stream
     * @throws EPException on any error creating the new window
     */
    EventChannel registerWindow(Window window, EPACallback callback, EventChannel input) throws EPException;

    /**
     * Deploys a projection match EPA based on the given definition
     *
     * @param def      the EPA definition
     * @param callback the callback to invoke for events leaving this EPA
     * @param input    the input-stream
     * @return a reference to the patter matcher's result-stream
     * @throws EPException on any error creating the new pattern matcher
     */
    EventChannel registerProjection(Projection def, EPACallback callback, EventChannel input) throws EPException;

    /**
     * Deploys a new filter EPA based on the given definition
     *
     * @param def      the EPA definition
     * @param callback the callback to invoke for events leaving this EPA
     * @param input    the input-stream
     * @return a reference to the filter's result-stream
     * @throws EPException on any error creating the new filter
     */
    EventChannel registerFilter(Filter def, EPACallback callback, EventChannel input) throws EPException;

    /**
     * Deploys a new aggregate EPA based on the given definition
     *
     * @param def      the EPA definition
     * @param callback the callback to invoke for events leaving this EPA
     * @param input    the input-stream
     * @return a reference to the aggregator's result-stream
     * @throws EPException on any error creating the new aggregator
     */
    EventChannel registerAggregator(Aggregator def, EPACallback callback, EventChannel input) throws EPException;

    /**
     * Deploys a new join EPA based on the given definition
     *
     * @param def      the EPA definition
     * @param callback the callback to invoke for events leaving this EPA
     * @param input1   the first input-stream
     * @param input2   the second input-stream
     * @return a reference to the correlator's result-stream
     * @throws EPException on any error creating the new correlator
     */
    EventChannel registerCorrelator(Correlator def, EPACallback callback, EventChannel input1, EventChannel input2) throws EPException;

    /**
     * Deploys a new pattern match EPA based on the given definition
     *
     * @param def      the EPA definition
     * @param callback the callback to invoke for events leaving this EPA
     * @param input    the input-stream
     * @return a reference to the patter matcher's result-stream
     * @throws EPException on any error creating the new pattern matcher
     */
    EventChannel registerPatternMatcher(PatternMatcher def, EPACallback callback, EventChannel input) throws EPException;

    /**
     * Removes the EPA represented by the given handle
     *
     * @param handle a reference to the EPA to remove
     * @param force  forces removal, even if there are consumers registered to this epa
     * @param flush  flag to tell the EPA to flush its state (e.g., incomplete windows, pending results)
     * @throws NoSuchQueryException if the EPA is defined
     */
    void removeEPA(EventChannel handle, boolean force, boolean flush) throws NoSuchQueryException;

    /**
     * Removes the stream represented by the given reference
     *
     * @param handle a reference to the stream to remove
     * @param force  forces removal, even if there are consumers registered to this stream
     * @throws NoSuchStreamException if the stream is not defined
     */
    void removeStream(EventChannel handle, boolean force) throws NoSuchStreamException;

    /**
     * Shuts down this bridge. Any subsequent calls to any of this bridge's methods will result
     * in undefined behaviour.
     */
    void destroy();

}
