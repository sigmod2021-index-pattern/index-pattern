package sigmod2021.esp.provider.impl.handler;

import sigmod2021.esp.api.epa.EPA;
import sigmod2021.esp.api.provider.EventSink;
import sigmod2021.esp.api.provider.QueryHandle;
import sigmod2021.event.EventSchema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class Query implements QueryHandle, Iterable<EPAHandler> {

    private final String name;

    private final EPA def;

    private final LinkedList<EPAHandler> epas = new LinkedList<>();

    private final List<EventSink> sinks = new ArrayList<>();

    private EventSchema outputSchema;

    public Query(String name, EPA def) {
        this.name = name;
        this.def = def;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public EventSchema getOutputSchema() {
        return outputSchema;
    }

    public EPA getDefinition() {
        return def;
    }

    public void addEPA(EPAHandler epa) {
        epa.increaseRefCount();
        // Rewire schema and sinks
        outputSchema = epa.getResultChannel().getSchema();
//		if ( !epas.isEmpty() ) 
//			epas.getLast().setSinks(null);
//		epa.setSinks(sinks);
        epas.add(epa);
    }

    public void addSink(EventSink sink) {
        sinks.add(sink);
        if (!epas.isEmpty())
            epas.getLast().setSinks(sinks);

    }

    public void removeSink(EventSink sink) {
        sinks.remove(sink);
        if (!epas.isEmpty())
            epas.getLast().setSinks(sinks);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<EPAHandler> iterator() {
        return epas.descendingIterator();
    }

    public Iterator<EPAHandler> bottomUpIterator() {
        return epas.iterator();
    }
}
