package sigmod2021.esp.bridges.nat.epa.util;

import sigmod2021.event.Event;

import java.util.Comparator;

/**
 * Simple comparator for timestamps on events
 */
public class AscTimestampComparator implements Comparator<Event> {

    @Override
    public int compare(Event e1, Event e2) {
        return Long.compare(e1.getT1(), e2.getT1());
    }

}
