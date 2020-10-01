package sigmod2021.esp.provider.impl.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sigmod2021.esp.api.bridge.EPACallback;
import sigmod2021.esp.api.bridge.EventChannel;
import sigmod2021.esp.api.epa.EPA;
import sigmod2021.esp.api.epa.UserDefinedEPA;
import sigmod2021.esp.api.provider.EventSink;
import sigmod2021.esp.api.util.GeoMapper;
import sigmod2021.event.Event;
import sigmod2021.event.EventSchema;

import java.util.ArrayList;
import java.util.List;

public class EPAHandler implements EPACallback {

    private static final Logger log = LoggerFactory.getLogger(EPAHandler.class);
    private final EPA def;
    /**
     * Mapper for geometric attributes
     */
    protected GeoMapper geoMapper;
    private EventChannel channel = null;
    private int refCount = 0;
    private List<EventSink> sinks;
    private List<UserDefinedEPA> udos = new ArrayList<>();

    public EPAHandler(EPA def) {
        this.def = def;
    }

    public EPAHandler(EPA def, EventChannel channel) {
        this.def = def;
        this.channel = channel;
    }

    public EPA getDefinition() {
        return def;
    }

    public void addUserDefinedOperator(UserDefinedEPA udo) {
        udos.add(udo);
    }

    public void removeUserDefinedOperator(UserDefinedEPA udo) {
        udos.remove(udo);
    }

    @Override
    public void receive(Event event) {
        if (log.isDebugEnabled())
            log.debug("Received event on {}: {}", def, event);

        if (sinks != null && !sinks.isEmpty()) {
            Event converted = null;
            for (EventSink sink : sinks) {
                if (!sink.convertGeometries())
                    sink.receiveEvent(event);
                else {
                    if (converted == null)
                        converted = geoMapper.mapOutgoing(event);
                    sink.receiveEvent(converted);
                }
            }
        }

        for (UserDefinedEPA udo : udos)
            udo.process(getResultChannel(), event);

        getResultChannel().push(event);
    }

    public void setSinks(final List<EventSink> sinks) {
        this.sinks = sinks;
    }

    public final EventChannel getResultChannel() {
        return channel;
    }

    public final void setResultChannel(EventChannel channel) {
        this.channel = channel;
        EventSchema schema = this.channel.getSchema();
        geoMapper = new GeoMapper(schema);
    }

    public EventSchema getSchema() {
        return this.channel.getSchema();
    }

    public void increaseRefCount() {
        refCount++;
    }

    public void decreaseRefCount() {
        refCount--;
    }

    public int getRefCount() {
        return refCount;
    }
}
