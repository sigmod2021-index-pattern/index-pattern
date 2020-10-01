package sigmod2021.esp.api.provider;

import sigmod2021.esp.api.epa.EPA;
import sigmod2021.event.EventSchema;

/**
 * Handle to a registered data-source.
 */
public interface DataSource {

    /**
     * @return the name of the represented event stream
     */
    String getName();

    /**
     * @return the event-schema of this stream
     */
    EventSchema getSchema();

    /**
     * @return an EPA to use as input for queries
     */
    EPA getEPA();

}
