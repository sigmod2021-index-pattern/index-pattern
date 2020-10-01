package sigmod2021.esp.bridges.nat.epa;

import sigmod2021.esp.api.bridge.EPACallback;
import sigmod2021.esp.api.bridge.EventChannel;
import sigmod2021.event.Event;
import sigmod2021.event.EventSchema;

public interface NativeOperator {

    EventSchema getSchema();

    void process(EventChannel input, Event event);

    void setCallback(EPACallback callback);

    void flushState();
}
