package sigmod2021.esp.bridges.nat.epa.impl.window;

import sigmod2021.esp.api.bridge.EventChannel;
import sigmod2021.esp.api.epa.window.TimeWindow;
import sigmod2021.event.Event;

/**
 * Implements a time-based window
 */
public class NativeTimeWindow extends NativeWindow {

    public NativeTimeWindow(EventChannel input, TimeWindow def) {
        super(input, def);
    }

    /**
     * @{inheritDoc
     */
    @Override
    public void process(EventChannel input, Event event) {
        long t1 = (event.getT1() / jump) * jump;
        long t2 = ((event.getT1() + size) / jump) * jump;
        if (t2 != t1)
            callback.receive(event.window(t1, t2));
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void flushState() {
    }

}
