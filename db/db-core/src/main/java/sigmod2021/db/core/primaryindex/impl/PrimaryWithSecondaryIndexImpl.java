package sigmod2021.db.core.primaryindex.impl;

import sigmod2021.db.DBException;
import sigmod2021.db.core.secondaryindex.LSMTimeIndex;
import sigmod2021.db.core.secondaryindex.SecondaryTimeIndex;
import sigmod2021.db.core.secondaryindex.SecondaryTimeIndexWrapper;
import sigmod2021.db.core.primaryindex.PrimaryIndex;
import sigmod2021.db.core.primaryindex.impl.legacy.ImmutableParams;
import sigmod2021.db.core.primaryindex.impl.legacy.MutableParams;
import sigmod2021.db.core.primaryindex.queries.range.AttributeRange;
import sigmod2021.db.event.PersistentEvent;
import sigmod2021.db.event.TID;
import sigmod2021.db.queries.NoSuchEventException;
import sigmod2021.db.util.TimeInterval;
import sigmod2021.event.*;
import xxl.core.cursor.DoubleCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.sources.EmptyCursor;
import xxl.core.indexStructures.AggregateIndex;
import xxl.core.indexStructures.FastAggregatedBPlusTree;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class PrimaryWithSecondaryIndexImpl implements PrimaryIndex {

    private PrimaryIndexImpl primary;
    private Map<String, SecondaryTimeIndex<?>> secondaryTimeIndices;

    public PrimaryWithSecondaryIndexImpl(final Path storageDirectory, final EventSchema schema, final TimeRepresentation timeRepresentation,
                                         final ImmutableParams ip, MutableParams mp) throws DBException {
        System.out.println("Secondary TAB Plus");
        this.primary = new PrimaryIndexImpl(storageDirectory, schema, timeRepresentation, ip, mp);
        this.secondaryTimeIndices = new HashMap<>();
        for (Attribute attribute : schema) {
            if (attribute.hasProperty("secondaryIndex")) {
                System.out.println("Creating index for attribute " + attribute.getName());
                secondaryTimeIndices.put(attribute.getName(),
                        new LSMTimeIndex(
                                storageDirectory,
                                primary.getSchema(),
                                attribute.getName(),
                                primary.getTree().BLOCK_SIZE));
                System.out.println("Created a LSM Index...");
            }
        }
        System.out.println("Created " + secondaryTimeIndices.size() + " LSM Indices");
    }

    @Override
    public TimeInterval getCoveredTimeInterval() {
        return primary.getCoveredTimeInterval();
    }

    @Override
    public int getMaxLeafEntries() {
        return primary.getMaxLeafEntries();
    }

    @Override
    public int getMaxIndexEntries() {
        return primary.getMaxIndexEntries();
    }

    @Override
    public long getNumberOfEvents() {
        return primary.getNumberOfEvents();
    }

    @Override
    public long getSerializedEventSize() {
        return primary.getSerializedEventSize();
    }

    @Override
    public long getContainerSize() {
        return primary.getContainerSize();
    }

    @Override
    public EventSchema getSchema() {
        return primary.getSchema();
    }

    @Override
    public Future<TID> insert(Event event) {
        Future<TID> result = primary.insert(event);
        // TODO: This is a hack
        final long seqNumber = primary.getNumberOfEvents() - 1;

        try {
            TID tid = result.get();

            for (SecondaryTimeIndex<?> secondaryTimeIndex : secondaryTimeIndices.values()) {
                secondaryTimeIndex.insertEvent(event, tid, seqNumber);
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public DoubleCursor<PersistentEvent> find(TID id) throws NoSuchEventException {
        return primary.find(id);
    }

    @Override
    public DoubleCursor<PersistentEvent> query(long minKey, long maxKey) {
        return primary.query(minKey, maxKey);
    }

    @Override
    public Cursor<PersistentEvent> querySecondaryAttributes(long minKey, long maxKey, AttributeRange<?>... ranges) throws
            SchemaException {
        if (ranges.length == 1 && secondaryTimeIndices.containsKey(ranges[0].getName())) {
            return querySecondaryIndexAttribute(minKey, maxKey, ranges[0]);
        }
        System.out.println("Querying lightweight index for " + ranges[0].getName() + " because ranges has keys:" + secondaryTimeIndices.keySet());
        return new EmptyCursor<>();
//        return tabPlusTree.querySecondaryAttributes(minKey, maxKey, ranges);
    }

    public Cursor<PersistentEvent> querySecondaryAttributesValueOrder(AttributeRange<?>... ranges) throws
            SchemaException {
        return querySecondaryAttributesValueOrder(Long.MIN_VALUE, Long.MAX_VALUE, ranges);
    }

    public Cursor<PersistentEvent> querySecondaryAttributesValueOrder(long minKey, long maxKey, AttributeRange<?>... ranges) throws
            SchemaException {
        if (ranges.length == 1 && secondaryTimeIndices.containsKey(ranges[0].getName())) {
            return querySecondaryIndexAttributeValueOrderd(minKey, maxKey, ranges[0]);
        }
        System.out.println("Querying lightweight index for " + ranges[0].getName() + " because ranges has keys:" + secondaryTimeIndices.keySet());
        return new EmptyCursor<>();
    }

    private Cursor<PersistentEvent> querySecondaryIndexAttribute(long minKey, long maxKey, AttributeRange<?> range) {
        System.out.println("Querying heavyweight index for " + range.getName());
        primary.appender.getTempRoot(true);
        SecondaryTimeIndex<?> secondaryTimeIndex = secondaryTimeIndices.get(range.getName());
        Cursor<PersistentEvent> eventIterator = new SecondaryTimeIndexWrapper(primary.getTree(), secondaryTimeIndex).rangeQuery(range.getLower(), range.getUpper());
        return eventIterator;
    }

    private Cursor<PersistentEvent> querySecondaryIndexAttributeValueOrderd(long minKey, long maxKey, AttributeRange<?> range) {
        System.out.println("Querying heavyweight index for " + range.getName());
        primary.appender.getTempRoot(true);
        SecondaryTimeIndex<?> secondaryTimeIndex = secondaryTimeIndices.get(range.getName());
        Cursor<PersistentEvent> eventIterator = new SecondaryTimeIndexWrapper(primary.getTree(), secondaryTimeIndex).rangeQueryValueOrderd(range.getLower(), range.getUpper());
        return eventIterator;
    }

    @Override
    public AggregateIndex.Aggregation getAggregates(String attribute, long minKey, long maxKey) {
        return primary.getAggregates(attribute, minKey, maxKey);
    }

    public PrimaryIndexImpl.State getState() {
        return primary.getState();
    }

    @Override
    public void flush() {
        primary.flush();
    }

    @Override
    public void close() {
        primary.close();
        for (SecondaryTimeIndex<?> sti : secondaryTimeIndices.values())
            sti.close();
    }

    public FastAggregatedBPlusTree getTree() {
        return primary.getTree();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public int getHeight() {
        return primary.getHeight();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public long estimateIO(long numPages) {
        return primary.estimateIO(numPages);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Cursor<Cursor<PersistentEvent>> query(Cursor<TimeInterval> intervals) {
        return primary.query(intervals);
    }


}
