package sigmod2021.db.core.secondaryindex;


import sigmod2021.db.event.TID;
import sigmod2021.event.Event;
import xxl.core.cursors.Cursor;
import xxl.core.util.Pair;

import java.util.Iterator;

/**
 * Models a secondary index for events.
 */
public interface SecondaryTimeIndex<K extends Comparable<K>> {

    /**
     * Creates an index on the whole stream, i.e. calling
     * <br />
     * <code>&nbsp;&nbsp;build(Long.MIN_VALUE, Long.MAX_VALUE)</code>
     * <br />
     */
    void build();

    /**
     * Creates an index on events from the given timestamp on.
     * <br />
     * <code>&nbsp;&nbsp;build(startTime, Long.MAX_VALUE)</code>
     * <br />
     *
     * @param startTime The start time for events to be indexed.
     */
    void build(long startTime);

    /**
     * Creates an index for the time interval given
     * by [startTime, endTime]. Only events within the given time interval
     * are indexed.
     *
     * @param startTime the left border of the time interval
     * @param endTime   the right border of the time interval
     */
    void build(long startTime, long endTime);

    /**
     * @reteurn the name of the indexed attribute
     */
    String getAttributeName();

    default Pair<K, EventID> exactMatchQueryID(K key) {
        Iterator<Pair<K, EventID>> ev = rangeQueryID(key, key);
        if (ev.hasNext())
            return ev.next();
        else
            return null;
    }

    /**
     * Executes a range query on this index structure and returns the events
     * between minKey and maxKey, i.e. all events <code>e</code> with
     * <code>minKey <= e.key <= maxKey</code>. The results are obtained from
     * the underlying structure through cursor that requests the next result on demand.
     *
     * @param minKey the minimum key of the requested events
     * @param maxKey the maximum key of the requested events
     * @return the query results
     */
    Cursor<Pair<K, EventID>> rangeQueryID(K minKey, K maxKey);


    /**
     * Inserts a new event in this index structure.
     * The tid consists of the block address of the given
     * event in the underlying container and of the offset
     * within the block.
     *
     * @param event the event to be inserted
     * @param tid   the location in the underlying container
     */
    void insertEvent(Event event, TID tid, long sequenceId);

    /**
     * Discards all events prior to the given time stamp.
     *
     * @param timeStamp the time stamp
     */
    void discardEvents(long timeStamp) throws UnsupportedOperationException;

    /**
     * Destroys and removes the whole index from disk.
     *
     * @throws UnsupportedOperationException
     */
    void destroy() throws UnsupportedOperationException;

    void close();

    /**
     * Estimates the I/O in bytes for a range-query with the given selectivity
     *
     * @param selectivity the selectivity
     * @return the expected IO in bytes
     */
    double estimateIO(double selectivity);

}
