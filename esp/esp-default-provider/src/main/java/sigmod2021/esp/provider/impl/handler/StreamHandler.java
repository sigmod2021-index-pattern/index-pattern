package sigmod2021.esp.provider.impl.handler;

import sigmod2021.esp.api.bridge.EventChannel;
import sigmod2021.esp.api.epa.Stream;
import sigmod2021.esp.api.provider.ExternalStreamHandle;
import sigmod2021.esp.api.provider.StreamHandle;
import sigmod2021.event.Event;
import sigmod2021.event.impl.SimpleEvent;

public class StreamHandler extends EPAHandler implements StreamHandle, ExternalStreamHandle {

    private final String name;

    public StreamHandler(String name, EventChannel channel) {
        super(new Stream(name));
        this.name = name;
        setResultChannel(channel);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void pushEvent(Object[] payload, long timestamp) {
        Object[] mapped = geoMapper.mapIncoming(payload);
        Event event = new SimpleEvent(mapped, timestamp, timestamp + 1);
        receive(event);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pushEvent(Event event) {
        receive(event);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Stream getEPA() {
        return new Stream(this.name);
    }

}
