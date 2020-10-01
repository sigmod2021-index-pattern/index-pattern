package sigmod2021.esp.api.provider;

import sigmod2021.esp.api.EPProvider;
import sigmod2021.event.EventSchema;


/**
 * Handle to a deployed query. This is returned by the active {@link EPProvider}
 * after successful deployment of a query. The handle may be used
 * to register {@link EventSink sinks} or to remove the query.
 */
public interface QueryHandle {

    /**
     * @return the name of the represented query
     */
    String getName();

    /**
     * @return the schem of this query's result events
     */
    EventSchema getOutputSchema();

}
