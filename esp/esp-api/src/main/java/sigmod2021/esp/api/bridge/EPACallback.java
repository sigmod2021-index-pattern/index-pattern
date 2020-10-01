package sigmod2021.esp.api.bridge;

import sigmod2021.esp.api.EPProvider;
import sigmod2021.event.Event;

/**
 * Callback used by EPA-instances to report results
 */
public interface EPACallback {

    /**
     * Reports results to the running {@link EPProvider}
     *
     * @param event the result event
     */
    void receive(Event event);

}
