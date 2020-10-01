package sigmod2021.db.core.primaryindex;

import sigmod2021.db.core.primaryindex.queries.range.AttributeRange;
import sigmod2021.db.event.PersistentEvent;
import sigmod2021.db.event.TID;
import sigmod2021.db.queries.NoSuchEventException;
import sigmod2021.db.util.TimeInterval;
import sigmod2021.event.Event;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;
import xxl.core.cursor.DoubleCursor;
import xxl.core.cursors.Cursor;
import xxl.core.indexStructures.AggregateIndex.Aggregation;

import java.util.List;
import java.util.concurrent.Future;

/**
 * TABPlusTree interface.
 */
public interface PrimaryIndex {

    /**
     * @return an interval [a,b] describing the range of timestamps stored in this tree
     */
    TimeInterval getCoveredTimeInterval();

    int getMaxLeafEntries();

    int getMaxIndexEntries();

    long getNumberOfEvents();

    long getSerializedEventSize();

    long getContainerSize();

    int getHeight();

    long estimateIO(long numPages);

    /**
     * @return The event-schema of events stored inside this tree
     */
    EventSchema getSchema();

    /**
     * Inserts the given event into this TAB+-Tree.
     *
     * @param event
     *            the event to insert
     * @return a future holding the event's {@link TID} once it was written to
     *         disk
     */
    Future<TID> insert(Event event);

    /**
     * Finds the event with the given it and returns a cursor pointing to it
     * @param id the id of the first event
     * @return a cursor pointing to the event with the given id
     * @throws NoSuchEventException if no event with the given id exists.
     */
    DoubleCursor<PersistentEvent> find(TID id) throws NoSuchEventException;

    /**
     * @param minKey
     *            the minimum key to include (inclusive)
     * @param maxKey
     *            the maximum key to include (inclusive)
     * @return All events with keys in the specified range in chronological
     *         order
     */
    DoubleCursor<PersistentEvent> query(long minKey, long maxKey);

    Cursor<Cursor<PersistentEvent>> query(Cursor<TimeInterval> intervals);

    /**
     * Performs a multi-dimensional range-query with respect to the given time
     * bound
     *
     * @param minKey
     *            the lower time bound (inclusive)
     * @param maxKey
     *            the upper time bound (inclusive)
     * @param ranges
     *            The attribute ranges to query
     * @return all events inside the given hypercube
     * @throws SchemaException
     *             if the given ranges do not match the tree's schema
     */
    Cursor<PersistentEvent> querySecondaryAttributes(long minKey, long maxKey, AttributeRange<?>... ranges) throws SchemaException;

    /**
     * Retrieves all available aggregates over all events in the given key range
     *
     * @param attribute
     *            the name of the attribute to retrieve the aggregates for
     * @param minKey
     *            the lower key-bound (inclusive)
     * @param maxKey
     *            the upper key-bound (inclusive)
     * @return the aggregate values of for the desired attribute and key range
     */
    Aggregation getAggregates(String attribute, long minKey, long maxKey);

    /**
     * Closes this tree instance. Calling any method after a call to close
     * results in undefined behavior.
     */
    void close();

    /**
     * Flushes all buffered pages
     */
    void flush();

    /////////////////////////////////////////////////////////////////
    //
    // CONVENIENCE METHODS
    //
    /////////////////////////////////////////////////////////////////

    /**
     * Retrieves the event with the given id
     * @param id the event's id
     * @return the event with the given id
     * @throws NoSuchEventException if the given id does not point to an event
     */
    default PersistentEvent get(TID id) throws NoSuchEventException {
        try (DoubleCursor<PersistentEvent> res = find(id)) {
            res.open();
            return res.next();
        }
    }

    /**
     * @return All events stored in this tree in chronological order
     */
    default DoubleCursor<PersistentEvent> query() {
        return query(Long.MIN_VALUE, Long.MAX_VALUE);
    }

    /**
     * Performs a multi-dimensional range-query with no time bound
     *
     * @param ranges
     *            The attribute ranges to query
     * @return all events inside the given hypercube
     * @throws SchemaException
     *             if the given ranges do not match the tree's schema
     */
    default Cursor<PersistentEvent> querySecondaryAttributes(final AttributeRange<?>... ranges) throws SchemaException {
        return querySecondaryAttributes(Long.MIN_VALUE, Long.MAX_VALUE, ranges);
    }

    /**
     * Performs a multi-dimensional range-query with respect to the given time
     * bound
     *
     * @param minKey
     *            the lower time bound (inclusive)
     * @param maxKey
     *            the upper time bound (inclusive)
     * @param ranges
     *            The attribute ranges to query
     * @return all events inside the given hypercube
     * @throws SchemaException
     *             if the given ranges do not match the tree's schema
     */
    default Cursor<PersistentEvent> querySecondaryAttributes(final long minKey, final long maxKey,
                                                             final List<AttributeRange<?>> ranges) throws SchemaException {
        return querySecondaryAttributes(minKey, maxKey, ranges.toArray(new AttributeRange[ranges.size()]));
    }

    /**
     * Performs a multi-dimensional range-query with no time bound
     *
     * @param ranges
     *            The attribute ranges to query
     * @return all events inside the given hypercube
     * @throws SchemaException
     *             if the given ranges do not match the tree's schema
     */
    default Cursor<PersistentEvent> querySecondaryAttributes(final List<AttributeRange<?>> ranges) throws SchemaException {
        return querySecondaryAttributes(Long.MIN_VALUE, Long.MAX_VALUE, ranges);
    }

    /**
     * Retrieves the desired aggregate over all events stored in this tree
     *
     * @param name
     *            the name of the aggregate (e.g., sum, count, avg, ...)
     * @param attribute
     *            the name of the attribute to retrieve the aggregate for
     * @return the aggregate value
     */
    default Number getAggregate(final String name, final String attribute) {
        return getAggregate(name, attribute, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    /**
     * Retrieves the desired aggregate over all events in the given key range
     *
     * @param name
     *            the name of the aggregate (e.g., sum, count, avg, ...)
     * @param attribute
     *            the name of the attribute to retrieve the aggregate for
     * @param minKey
     *            the lower key-bound (inclusive)
     * @param maxKey
     *            the upper key-bound (inclusive)
     * @return the aggregate value
     */
    default Number getAggregate(final String name, final String attribute, final long minKey, final long maxKey) {
        // TODO: Implementation dependent
        final Number[] res = getAggregates(attribute, minKey, maxKey).getValues();
        switch (name.toLowerCase()) {
            case "sum":
                return res[0].doubleValue();
            case "min":
                return res[1].doubleValue();
            case "max":
                return res[2].doubleValue();
            case "count":
                return res[3].longValue();
            case "avg":
                return res[0].doubleValue() / res[3].doubleValue();
            default:
                throw new IllegalArgumentException("Unknown aggregate: " + name);
        }
    }

    /**
     * Retrieves all available aggregates over all events stored in this tree
     *
     * @param attribute
     *            the name of the attribute to retrieve the aggregates for
     * @return the aggregate values of for the desired attribute
     */
    default Aggregation getAggregates(final String attribute) {
        return getAggregates(attribute, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    /**
     * @return An estimate of this tree compression ratio
     */
    default double getCompressionRatio() {
        long eventCount = getNumberOfEvents();
        long eventSize = getSerializedEventSize();
        long containerSize = getContainerSize();
        return (double) containerSize / (eventCount * eventSize);
    }

}
