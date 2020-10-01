package sigmod2021.pattern.experiments.util;

import sigmod2021.db.DBException;
import sigmod2021.db.core.IOMeasuring;
import sigmod2021.db.core.primaryindex.impl.PrimaryIndexImpl;
import sigmod2021.db.core.primaryindex.impl.legacy.ImmutableParams;
import sigmod2021.db.core.primaryindex.impl.legacy.MutableParams;
import sigmod2021.db.event.PersistentEvent;
import sigmod2021.db.event.TID;
import sigmod2021.db.queries.NoSuchEventException;
import sigmod2021.db.util.TimeInterval;
import sigmod2021.event.EventSchema;
import sigmod2021.event.TimeRepresentation;
import xxl.core.collections.containers.io.SuspendableContainer;
import xxl.core.cursor.DoubleCursor;
import xxl.core.cursor.MinimalCursor;
import xxl.core.cursors.Cursor;
import xxl.core.indexStructures.BPlusTree.Node;
import xxl.core.io.converters.Converter;

import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;


/**
 *
 */
public class AccessCountingPrimaryIndex extends PrimaryIndexImpl implements IOMeasuring {

    private PageAccessCountingContainer counter;

    private AtomicLong leafCounter;

    private AtomicLong intervalCounter = new AtomicLong();

    /**
     * Creates a new AccessCountingTABPlusTree instance
     * @param storageDirectory
     * @param schema
     * @param timeRepresentation
     * @param ip
     * @param mp
     * @throws DBException
     */
    public AccessCountingPrimaryIndex(Path storageDirectory, EventSchema schema, TimeRepresentation timeRepresentation,
                                      ImmutableParams ip, MutableParams mp) throws DBException {
        super(storageDirectory, schema, timeRepresentation, ip, mp);
    }

    /**
     * Creates a new AccessCountingTABPlusTree instance
     * @param storageDirectory
     * @param cfg
     * @throws DBException
     */
    public AccessCountingPrimaryIndex(Path storageDirectory, MutableParams cfg) throws DBException {
        super(storageDirectory, cfg);
    }

    /**
     * Creates a new AccessCountingTABPlusTree instance
     * @param storageDirectory
     * @throws DBException
     */
    public AccessCountingPrimaryIndex(Path storageDirectory) throws DBException {
        super(storageDirectory);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void emptyBuffers() {
        getAppender().emptyTreeBuffer();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void resetMeasurement() {
        counter.resetCounters();
        intervalCounter.set(0L);
        leafCounter.set(0L);
    }

    public long getQueriedIntervals() {
        return intervalCounter.get();
    }

    @Override
    public DoubleCursor<PersistentEvent> query(long minKey, long maxKey) {
        intervalCounter.incrementAndGet();
        return super.query(minKey, maxKey);
    }

    @Override
    public Cursor<Cursor<PersistentEvent>> query(Cursor<TimeInterval> intervals) {
        final var src = super.query(intervals);
        return new MinimalCursor<>() {
            @Override
            public void open() {
                src.open();
            }

            @Override
            public void close() {
                src.close();
            }

            @Override
            public boolean hasNext() throws IllegalStateException {
                return src.hasNext();
            }

            @Override
            public Cursor<PersistentEvent> next() throws IllegalStateException, NoSuchElementException {
                intervalCounter.incrementAndGet();
                return src.next();
            }
        };
    }

    @Override
    public DoubleCursor<PersistentEvent> find(TID id) throws NoSuchEventException {
        intervalCounter.incrementAndGet();
        return super.find(id);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public long getTotalReadBytes() {
        return (long) (counter.getReadCount() * getTree().BLOCK_SIZE);// * getCompressionRatio());
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public long getTotalWrittenBytes() {
        return (long) (counter.getWriteCount() * getTree().BLOCK_SIZE);// * getCompressionRatio());
    }

    public long getReadLeafBytes() {
        return getLeafPagesRead() * getTree().BLOCK_SIZE;
    }

    public long getReadInnerBytes() {
        return getInnerPagesRead() * getTree().BLOCK_SIZE;
    }

    public long getLeafPagesRead() {
        return leafCounter.get();
    }

    public long getInnerPagesRead() {
        return counter.readCount.get() - leafCounter.get();
    }

    public long getPagesRead() {
        return counter.readCount.get();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    protected SuspendableContainer createContainer(Path containerPath, Converter<?> treeNodeConverter) {
        leafCounter = new AtomicLong();
        counter = new PageAccessCountingContainer(super.createContainer(containerPath, treeNodeConverter)) {

            @Override
            public Object get(Object id, boolean unfix) throws NoSuchElementException {
                Node n = (Node) super.get(id, unfix);
//				System.out.println(n.level() + " -> " + id);
                if (n.level == 0)
                    leafCounter.incrementAndGet();
                return n;
            }

            @Override
            public Object get(Object id) throws NoSuchElementException {
                Node n = (Node) super.get(id);
//				System.out.println(n.level() + " -> " + id);
                if (n.level == 0)
                    leafCounter.incrementAndGet();
                return n;
            }
        };
        return counter;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    protected SuspendableContainer loadContainer(Path containerPath, Converter<?> treeNodeConverter) {
        leafCounter = new AtomicLong();
        counter = new PageAccessCountingContainer(super.loadContainer(containerPath, treeNodeConverter)) {

            @Override
            public Object get(Object id, boolean unfix) throws NoSuchElementException {
                Node n = (Node) super.get(id, unfix);
//				System.out.println(n.level() + " -> " + id);
                if (n.level == 0)
                    leafCounter.incrementAndGet();
                return n;
            }

            @Override
            public Object get(Object id) throws NoSuchElementException {
                Node n = (Node) super.get(id);
//				System.out.println(n.level() + " -> " + id);
                if (n.level == 0)
                    leafCounter.incrementAndGet();
                return n;
            }
        };
        return counter;
    }


}
