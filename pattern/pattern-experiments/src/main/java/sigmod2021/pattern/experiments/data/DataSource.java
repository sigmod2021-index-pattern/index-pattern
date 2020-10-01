package sigmod2021.pattern.experiments.data;

import sigmod2021.event.Event;
import sigmod2021.event.EventSchema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

public interface DataSource extends Iterable<Event> {

    EventSchema getSchema();

    Iterator<Event> iterator();

    String getName();
}
