package sigmod2021.esp.bridges.nat.epa.impl.correlator;

import sigmod2021.esp.bridges.nat.epa.util.AscEndTimestampComparator;
import sigmod2021.event.Event;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Implements a Sweep-Area backed by a LinkedHashMap for start-timestamp
 * ordering and a Heap for end-timestamp ordering. We exploit the ordered nature
 * of incoming start-timestamps by using the LinkedHashMap.
 */
public class IntervalSweepArea {

    /**
     * Holds all events ordered by their insertion <=> ordered by tstart
     */
    private Set<Event> startOrderSet = new LinkedHashSet<Event>();
    /**
     * Holds all elements ordered by their end-timestamp for efficient cleanup
     */
    private PriorityQueue<Event> endOrderQueue = new PriorityQueue<Event>(new AscEndTimestampComparator());

    /**
     * Adds the given event to this SweepArea
     *
     * @param event the event to add
     */
    public void add(Event event) {
        startOrderSet.add(event);
        endOrderQueue.add(event);
    }
    ;

    /**
     * @return the size of this SweepArea
     */
    public int size() {
        return startOrderSet.size();
    }

    /**
     * Clears this SweepArea
     */
    public void clear() {
        startOrderSet.clear();
        endOrderQueue.clear();
    }

    /**
     * Removes all events with timestamps &lt;= the given one
     *
     * @param timestamp the upper bound of timestamps to remove (inclusive)
     */
    public void removeWithEndTimeStampLQ(long timestamp) {
        while (!endOrderQueue.isEmpty() && endOrderQueue.peek().getT2() <= timestamp) {
            startOrderSet.remove(endOrderQueue.poll());
        }
    }

    /**
     * Creates an iterator with all events having a start timestamp less than
     * the given one
     *
     * @param timestamp the upper limit (exlusive)
     * @return the iterator
     */
    public Iterator<Event> queryStartTimestampLess(long timestamp) {
        return new SAIterator(timestamp, startOrderSet.iterator());
    }

    /**
     * @{inheritDoc
     */
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append(String.format("IntervalSweepArea: [%n"));
        for (Event oa : startOrderSet) {
            result.append(String.format("  %s%n", oa));
        }
        result.append("]");
        return result.toString();
    }

    /**
     * Iterates the list of events up to the given bound
     */
    private class SAIterator implements Iterator<Event> {

        /**
         * The upper bound to iterate to
         */
        private final long upperBound;

        /**
         * The iterator holding all events of the sweep-area
         */
        private final Iterator<Event> parent;

        /**
         * Stores the next event to return
         */
        private Event next;

        /**
         * Constructs a new SAIterator-instance
         *
         * @param upperBound The upper bound to iterate to
         * @param parent     The iterator holding all events of the sweep-area
         */
        public SAIterator(long upperBound, Iterator<Event> parent) {
            super();
            this.upperBound = upperBound;
            this.parent = parent;
            this.next = getNext();
        }

        /**
         * Calculates the next element and returns it
         *
         * @return the next element to publish, null if iteration is finished
         */
        private Event getNext() {
            Event res = null;
            if (parent.hasNext()) {
                res = parent.next();
                if (res.getT1() >= upperBound)
                    res = null;
            }
            return res;
        }

        /**
         * @{inheritDoc
         */
        @Override
        public boolean hasNext() {
            return next != null;
        }

        /**
         * @{inheritDoc
         */
        @Override
        public Event next() {
            Event result = next;
            next = getNext();
            return result;
        }
    }
}
