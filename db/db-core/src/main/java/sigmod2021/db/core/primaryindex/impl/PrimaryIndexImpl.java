package sigmod2021.db.core.primaryindex.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sigmod2021.db.DBException;
import sigmod2021.db.DBRuntimeException;
import sigmod2021.db.core.primaryindex.HistogramAccess;
import sigmod2021.db.core.primaryindex.PrimaryIndex;
import sigmod2021.db.core.primaryindex.impl.legacy.*;
import sigmod2021.db.core.primaryindex.queries.range.AttributeRange;
import sigmod2021.db.core.primaryindex.queries.range.DoubleAttributeRange;
import sigmod2021.db.core.wal.Log;
import sigmod2021.db.core.wal.NoLog;
import sigmod2021.db.event.PersistentEvent;
import sigmod2021.db.event.TID;
import sigmod2021.db.queries.NoSuchEventException;
import sigmod2021.db.util.TimeInterval;
import sigmod2021.event.*;
import sigmod2021.event.Attribute.DataType;
import xxl.core.collections.containers.AbstractContainer;
import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.compression.Compressor;
import xxl.core.collections.containers.compression.FastCompressionContainer;
import xxl.core.collections.containers.compression.LZ4Compressor;
import xxl.core.collections.containers.compression.NoCompressor;
import xxl.core.collections.containers.io.FCBlockFileContainer;
import xxl.core.collections.containers.io.SuspendableContainer;
import xxl.core.collections.containers.io.SuspendableConverterContainer;
import xxl.core.cursor.DoubleCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.filters.Filter;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Functional.BinaryFunction;
import xxl.core.indexStructures.AggregateIndex.Aggregation;
import xxl.core.indexStructures.BPlusLink;
import xxl.core.indexStructures.BPlusLink.Node;
import xxl.core.indexStructures.FastAggregatedAppender;
import xxl.core.indexStructures.FastAggregatedBPlusTree;
import xxl.core.indexStructures.FastAggregatedBPlusTree.IndexEntry;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.Converters.SerializationMode;
import xxl.core.io.converters.FixedSizeConverter;
import xxl.core.io.converters.LongConverter;
import xxl.core.io.converters.MeasuredConverter;
import xxl.core.util.Pair;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 *
 */
@SuppressWarnings("deprecation")
public class PrimaryIndexImpl implements Closeable, AutoCloseable, PrimaryIndex {

    /** Legacy time attribute */
    public static final Attribute TSTART =
            new Attribute("tstart", DataType.LONG);

    ;
    /** Legacy time attribute */
    public static final Attribute TEND =
            new Attribute("tend", DataType.LONG);
    /** The name of the container file (without suffix) */
    public static final String CONTAINER_FILE = "data";
    /** The name of the stable (!) meta-data file */
    public static final String META_FILENAME = "meta.mf";
    /** The name of the working meta-data file */
    public static final String META_FILENAME_TMP = "meta.mf.tmp";
    /** The logger */
    static final Logger log = LoggerFactory
            .getLogger(PrimaryIndexImpl.class);
    /** Converter for keys. */
    private static final MeasuredConverter<Long> KEY_CONVERTER =
            new MeasuredConverter<Long>() {

                private static final long serialVersionUID =
                        1L;

                @Override
                public int getMaxObjectSize() {
                    return 8;
                }

                @Override
                public Long read(
                        final DataInput dataInput,
                        final Long timestamp)
                        throws IOException {
                    return LongConverter.DEFAULT_INSTANCE
                            .readLong(
                                    dataInput);
                }

                @Override
                public void write(
                        final DataOutput dataOutput,
                        final Long timestamp)
                        throws IOException {
                    LongConverter.DEFAULT_INSTANCE
                            .writeLong(
                                    dataOutput,
                                    timestamp);
                }
            };
    /**
     * Returns the key (that is the start timestamp) for a given event.
     */
    private static final Function<Event, Long> KEY_FUNCTION =
            new Function<Event, Long>() {

                @Override
                public Long apply(
                        final Event event) {
                    return event
                            .getT1();
                }
            };
    private static final BinaryFunction<TID, Event, PersistentEvent> GENERATE_OUT =
            new BinaryFunction<TID, Event, PersistentEvent>() {

                private static final long serialVersionUID =
                        1L;

                @Override
                public PersistentEvent invoke(
                        TID id,
                        Event event) {
                    return new PersistentEvent(
                            id, event);
                }
            };
    /** The filesystem path, this tree is stored */
    private final Path storageDirectory;
    /** All infos required to load the state from disk */
    PrimaryIndexMetaData metaData;
    /** The underlying BPlus-Tree */
    FastAggregatedBPlusTree tree;
    /** The appender */
    FastAggregatedAppender<Event, PersistentEvent> appender;
    SuspendableContainer container;
    /** The {@link State} of this tree */
    private State state = State.OPENING;

    public PrimaryIndexImpl(final Path storageDirectory) throws DBException {
        this(storageDirectory, null);
    }

    public PrimaryIndexImpl(final Path storageDirectory, MutableParams cfg) throws DBException {
        this.storageDirectory = storageDirectory.normalize();

        log.info("Loading TABPlusTree from: {}.", this.storageDirectory);

        final Path tmpMetaFile = this.storageDirectory.resolve(META_FILENAME_TMP);
        final Path metaFile = this.storageDirectory.resolve(META_FILENAME);

        try {
            // Tree is dirty -> Recover
            if (Files.exists(tmpMetaFile)) {
                state = State.RECOVERING;
                loadDirty(tmpMetaFile, cfg);
                // Write temp meta-data stating we are opened
                this.metaData.persist(tmpMetaFile);
            }
            // Tree is clean -> Load
            else if (Files.exists(metaFile)) {
                loadClean(metaFile, cfg);
                // Write temp meta-data stating we are opened
                this.metaData.persist(tmpMetaFile);
            } else
                throw new FileNotFoundException(String.format("Meta data file not found (%s)", metaFile));
        } catch (IOException cause) {
            throw new LoadException(
                    String.format("Could not load TAB+-Tree from directory: %s.", storageDirectory),
                    cause);
        }

        state = State.OPEN;
        log.info("Finished loading TABPlusTree from: " +
                        "\n  Storage-Directory   : {}" +
                        "\n  MetaData            : {}",
                this.storageDirectory, this.metaData);
        log.info(
                "Tree Properties:" +
                        "\n  EventsPerLeaf      : [{},{}]" +
                        "\n  IndexEntriesPerNode: [{},{}]",
                tree.getLeafNodeD(), tree.getLeafNodeB(),
                tree.getIndexNodeD(), tree.getIndexNodeB());
        log.info(
                "Appender Properties: " +
                        "\n  MaxLeafEntries : {}" +
                        "\n  maxIndexEntries: {}",
                appender.getMaxLeafEntries(), appender.getMaxIndexEntries()
        );
        log.info("Data Properties: " +
                "\n  Event count  : {}" +
                "\n  Time interval: {}", getNumberOfEvents(), getCoveredTimeInterval());
    }

    /**
     * Creates a new TABPlusTree inside the given directory
     *
     * @param storageDirectory
     *            the directory to store the tree in
     * @param schema
     *            the event schema
     * @param timeRepresentation
     *            the time-representation to use
     * @throws IOException
     */
    public PrimaryIndexImpl(final Path storageDirectory, final EventSchema schema,
                            final TimeRepresentation timeRepresentation, final ImmutableParams ip, final MutableParams mp)
            throws DBException {
        this.storageDirectory = storageDirectory.normalize();
        this.metaData = new PrimaryIndexMetaData(schema, timeRepresentation, ip, mp);

        log.info(
                "Creating new TABPlusTree: " +
                        "\n  Storage-Directory   : {}" +
                        "\n  Schema              : {}" +
                        "\n  Time-Represenstation: {}" +
                        "\n  ImmutableConfig     : {}" +
                        "\n  MutableConfig       : {}",
                this.storageDirectory, schema, timeRepresentation, ip, mp);

        final Path tmpMetaFile = this.storageDirectory.resolve(META_FILENAME_TMP);
        final Path metaFile = this.storageDirectory.resolve(META_FILENAME);

        try {
            if (!Files.exists(this.storageDirectory)) {
                log.debug("Storage-Directory does not exist. Creating.");
                Files.createDirectories(this.storageDirectory);
            } else if (Files.exists(metaFile) || Files.exists(tmpMetaFile)) {
                log.error("Found MetaData-File in storage directory. Aborting.");
                throw new InitialializationException(
                        String.format("There already is a TABPlusTree in %s.", this.storageDirectory.toString()));
            }

            this.metaData.persist(tmpMetaFile);

        } catch (IOException cause) {
            throw new InitialializationException(
                    String.format("Could not create TAB+-Tree in directory: %s.", storageDirectory),
                    cause);
        }

        // TODO: Config params
        final Log<Event> WAL = new NoLog<>();
        final Log<Event> OOLog = new NoLog<>();

        this.tree = createTree(WAL);
        this.appender = createAppender(this.tree, WAL, OOLog);
        state = State.OPEN;

        log.info(
                "Tree Properties:" +
                        "\n  EventsPerLeaf      : [{},{}]" +
                        "\n  IndexEntriesPerNode: [{},{}]",
                tree.getLeafNodeD(), tree.getLeafNodeB(),
                tree.getIndexNodeD(), tree.getIndexNodeB());
        log.info(
                "Appender Properties: " +
                        "\n  MaxLeafEntries : {}" +
                        "\n  maxIndexEntries: {}",
                appender.getMaxLeafEntries(), appender.getMaxIndexEntries());
        log.info("Finished creating new TABPlusTree at: {}.", this.storageDirectory);
    }

    private static boolean intersectsAll(IndexEntry e, List<? extends AttributeRange<? extends Number>> ranges) {
        return ranges.stream().allMatch(x -> intersectsSingle(e, x));
    }

    private static boolean intersectsSingle(IndexEntry e, AttributeRange<? extends Number> range) {
        var agg = e.getAggregation(range.getName());
        var idxRange = new DoubleAttributeRange(range.getName(), agg.getValues()[1].doubleValue(),
                agg.getValues()[2].doubleValue(), true, true);
        var givenRange = new DoubleAttributeRange(range.getName(), range.getLower().doubleValue(),
                range.getUpper().doubleValue(), range.isLowerInclusive(), range.isUpperInclusive());
        return givenRange.intersects(idxRange);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public TimeInterval getCoveredTimeInterval() {
        return new TimeInterval(this.metaData.getMinTimestamp(), this.metaData.getMaxTimestamp());
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public int getMaxLeafEntries() {
        return appender.getMaxLeafEntries();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public int getMaxIndexEntries() {
        return appender.getMaxIndexEntries();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public long getNumberOfEvents() {
        return this.metaData.getEventCount();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public int getHeight() {
        ensureState(State.OPEN);
        return appender.getTempRoot(false).getElement1().level();
    }

    private long computeEventCount() {
        Attribute indexed = null;
        for (Attribute a : this.metaData.getSchema()) {
            if (a.getProperty("index") != null && Boolean.valueOf(a.getProperty("index"))) {
                indexed = a;
                break;
            }
        }

        if (indexed != null) {
            return this.tree.getAggregate(
                    indexed.getName(),
                    Long.MIN_VALUE,
                    Long.MAX_VALUE,
                    (FastAggregatedBPlusTree.Node) this.appender.getTempRoot(true).getSecond(),
                    this.metaData.getMinTimestamp()).getValues()[3].longValue();
        } else {
            long count = 0L;
            Cursor<PersistentEvent> c = this.appender.query(Long.MIN_VALUE, Long.MAX_VALUE);
            try {
                c.open();
                while (c.hasNext()) {
                    count++;
                    c.next();
                }
                c.close();
                return count;
            } finally {
                try {
                    c.close();
                } catch (Exception e) {
                }
            }
        }

    }

    /**
     * @{inheritDoc}
     */
    @Override
    public long getSerializedEventSize() {
        return SchemaManager.calculateEventSize(getSchema(), metaData.getTimeRepresentation());
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public long getContainerSize() {
        try {
            return Files.size(storageDirectory.resolve("data.ctr"));
        } catch (IOException e) {
            log.error("Unable to determine container size.", e);
        }
        return 0;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public EventSchema getSchema() {
        return this.metaData.getSchema();
    }

    ///////////////////////////////////////////////////////////////////////
    //
    // Find Query
    //
    ///////////////////////////////////////////////////////////////////////

    /**
     * @{inheritDoc}
     */
    @Override
    public Future<TID> insert(final Event event) {
        ensureState(State.OPEN);
        Future<TID> result;

        //		final Object[] legacyEvent = toLegacyEvent(event);

        if (log.isTraceEnabled()) {
            log.trace("Inserting event: {}", event);
        }

        if (this.metaData.isOutOfOrder(event)) {
            result = this.appender.outOfOrderInsert(event);
        } else {
            result = this.appender.insertEntry(event);
        }

        this.metaData.updateTimestamps(event);
        this.metaData.increaseEventCount();
        return result;
    }

    ///////////////////////////////////////////////////////////////////////
    //
    // Temporal queries
    //
    ///////////////////////////////////////////////////////////////////////

    /**
     * @{inheritDoc}
     */
    @Override
    public DoubleCursor<PersistentEvent> find(TID id) throws NoSuchEventException {
        ensureState(State.OPEN);
        return appender.find(id);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public DoubleCursor<PersistentEvent> query(final long minKey, final long maxKey) {
        ensureState(State.OPEN);
        long time = 0L;
        if (log.isDebugEnabled()) {
            time = -System.currentTimeMillis();
            log.debug("Executing range query: [{}, {}]", minKey, maxKey);
        }

        final DoubleCursor<PersistentEvent> queryResult = this.appender.query(minKey, maxKey);

        if (log.isDebugEnabled()) {
            log.debug("Finished executing range query: [{}, {}]. Execution time: {} ms", minKey, maxKey,
                    (time + System.currentTimeMillis()));
        }
        return queryResult;
    }

    ///////////////////////////////////////////////////////////////////////
    //
    // Attribute Queries
    //
    ///////////////////////////////////////////////////////////////////////

    public Cursor<Cursor<PersistentEvent>> query(Cursor<TimeInterval> intervals) {
        ensureState(State.OPEN);
        long time = 0L;
        if (log.isDebugEnabled()) {
            time = -System.currentTimeMillis();
            log.debug("Executing multi interval query");
        }

        var root = appender.getTempRoot(true);
        final Cursor<Cursor<PersistentEvent>> queryResult =
                new MultiIntervalCursor(intervals, metaData.getMinTimestamp(), root);

        if (log.isDebugEnabled()) {
            log.debug("Finished executing multi interval query. Execution time: {} ms",
                    (time + System.currentTimeMillis()));
        }
        return queryResult;
    }

    ///////////////////////////////////////////////////////////////////////
    //
    // Aggregate Queries
    //
    ///////////////////////////////////////////////////////////////////////

    /**
     * @{inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public Cursor<PersistentEvent> querySecondaryAttributes(final long minKey, final long maxKey,
                                                            final AttributeRange<?>... ranges)
            throws SchemaException {
        ensureState(State.OPEN);
        long time = 0L;
        if (log.isDebugEnabled()) {
            time = -System.currentTimeMillis();
            log.debug("Executing secondary attribute query. Key-range: [{}, {}], ranges: {}", minKey, maxKey,
                    Arrays.toString(ranges));
        }

        // No ranges? --> Fallback to history query
        if (ranges.length == 0)
            return query(minKey, maxKey);

        // Validate ranges
        for (final AttributeRange<?> range : ranges) {
            final Attribute attr = this.metaData.getSchema().byName(range.getName());
            if (attr.getType() != range.getDataType())
                throw new SchemaException(String.format(
                        "Incompatible data type for attribute \"%s\". Schema type is %s, but range type is %s",
                        attr.getName(), attr.getType(), range.getDataType()));

        }

        final LongKeyRange keys = new LongKeyRange(minKey, maxKey);
        final Map<String, AttributeRange<? extends Number>> transformed = new HashMap<>();

        // Transform for appender
        for (final AttributeRange<?> r : ranges) {
            if (r.getDataType().isNumeric())
                transformed.put(r.getName(), (AttributeRange<? extends Number>) r);
            else
                log.warn("Range {} not supported.", r);
        }

        final Cursor<PersistentEvent> queryResult = this.appender.queryAttributes(keys, transformed);

        // We need to filter invalid entries, since the cursor returns all results of a leaf
        // which interects the given ranges

        final AttributeRange.EventMatcher[] matchers = new AttributeRange.EventMatcher[ranges.length];
        for (int i = 0; i < ranges.length; i++) {
            matchers[i] = ranges[i].getMatcher(getSchema());
        }

        final Cursor<PersistentEvent> filtered =
                new Filter<>(queryResult, new xxl.core.functions.AbstractFunction<PersistentEvent, Boolean>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Boolean invoke(final PersistentEvent argument) {
                        if (argument.getT1() < minKey || argument.getT1() > maxKey)
                            return false;

                        for (final AttributeRange.EventMatcher matcher : matchers) {
                            if (!matcher.matches(argument))
                                return false;
                        }
                        return true;
                    }
                });

        if (log.isDebugEnabled()) {
            log.debug(
                    "Finished executing secondary attribute query. Key-range: [{}, {}], ranges: {}. Execution time: {} ms",
                    minKey, maxKey,
                    Arrays.toString(ranges), (time + System.currentTimeMillis()));
        }

        return filtered;
    }

    ///////////////////////////////////////////////////////////////////////
    //
    // Management stuff
    //
    ///////////////////////////////////////////////////////////////////////

    /**
     * @{inheritDoc}
     */
    @Override
    public Aggregation getAggregates(final String attribute, final long minKey, final long maxKey) {
        ensureState(State.OPEN);
        long time = 0L;
        Aggregation result = null;
        if (log.isDebugEnabled()) {
            time = -System.currentTimeMillis();
            log.debug("Executing aggregate query for attribute {}. Key-range: [{}, {}]", attribute, minKey, maxKey);
        }

        try {
            Attribute attr = this.metaData.getSchema().byName(attribute);
            // Curde empty check
            if (this.appender.getFirst() == null)
                result = new FastAggregatedBPlusTree.AggregationEntry();
            else
                result = this.tree.getAggregate(
                        attr.getName(),
                        Long.valueOf(minKey),
                        Long.valueOf(maxKey),
                        (FastAggregatedBPlusTree.Node) this.appender.getTempRoot(true).getSecond(),
                        this.metaData.getMinTimestamp());
        } catch (final SchemaException se) {
            throw new IllegalArgumentException(se);
        }

        if (log.isDebugEnabled()) {
            log.debug("Finished Executing aggregate query for attribute {}. Key-range: [{}, {}]", attribute, minKey,
                    maxKey,
                    (time + System.currentTimeMillis()));
        }

        return result;
    }

    /**
     * @return the state
     */
    public State getState() {
        return this.state;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void flush() {
        ensureState(State.OPEN);
        appender.flushPath();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void close() {
        if (state == State.CLOSED)
            return;
        ensureState(State.OPEN);

        log.info("Closing TABPlusTree at: {}", this.storageDirectory);

        log.debug("Flushing appender.");
        this.appender.flushPathAndClose();

        log.debug("Closing container.");
        this.tree.container().close();

        final Path tmpMetaFile = this.storageDirectory.resolve(META_FILENAME_TMP);
        final Path metaFile = this.storageDirectory.resolve(META_FILENAME);

        this.metaData.setTreeRoot(this.tree.rootEntry());

        log.debug("Writing state information.");
        try {
            this.metaData.persist(tmpMetaFile);
            // Replace existing file atomically
            Files.move(tmpMetaFile, metaFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            state = State.CLOSED;
        } catch (final IOException ioex) {
            log.error("Could not write state information to " + META_FILENAME + ". Some data might get lost.", ioex);
            state = State.ERROR;
        }
        log.info("Finished closing TABPlusTree at: {}", this.storageDirectory);
    }

    ///////////////////////////////////////////////////
    //
    // Create Tree Stuff
    //
    ///////////////////////////////////////////////////

    /**
     * Ensures the tree is in the given state
     *
     * @param state
     *            the required state
     * @throws IllegalStateException
     *             if the tree is not in the required state.
     */
    void ensureState(State state) {
        if (this.state != state)
            throw new IllegalStateException(
                    String.format("Illegal state for this operation. Required: %s, current: %s.", state, this.state));
    }

    /**
     * Creates a new empty tree instance, backed by the given WAL
     *
     * @param wal
     *            the WAL to use
     * @return a new empty tree instance
     */
    private FastAggregatedBPlusTree createTree(final Log<Event> wal) {

        // Create a tree
        final FastAggregatedBPlusTree tree = new FastAggregatedBPlusTree(this.metaData.getSchema(),
                this.metaData.getTimeRepresentation(),
                this.metaData.getImmutableParams().getBlockSize(), true, wal);

        // Create a container
        final Path path = this.storageDirectory.resolve(CONTAINER_FILE);

        container = createContainer(path, tree.nodeConverter());

        // Initialize Tree
        initializeEmptyTree(tree, container);
        return tree;
    }

    /**
     * Initializes the given empty tree
     *
     * @param tree
     *            the tree
     * @param treeContainer
     *            the container used for persistence
     */
    private void initializeEmptyTree(final FastAggregatedBPlusTree tree, final SuspendableContainer treeContainer) {
        tree.initialize(null, null, new AbstractFunction<Event, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long invoke(final Event argument) {
                        return PrimaryIndexImpl.KEY_FUNCTION.apply(argument);
                    }
                }, treeContainer, KEY_CONVERTER,
                SchemaManager.getDataConverter(metaData.getSchema(), metaData.getTimeRepresentation()),
                LongSeparator.FACTORY_FUNCTION,
                LongKeyRange.FACTORY_FUNCTION);
    }

    ///////////////////////////////////////////////////
    //
    // Load Stuff
    //
    ///////////////////////////////////////////////////

    /**
     * Creates a new container at the give path, using the given converter for
     * tree-nodes
     *
     * @param containerPath
     *            the path to store the container at
     * @param treeNodeConverter
     *            the tree node converter
     * @return the newly created container instance
     */
    protected SuspendableContainer createContainer(final Path containerPath, final Converter<?> treeNodeConverter) {
        ImmutableParams ip = this.metaData.getImmutableParams();
        MutableParams mp = this.metaData.getMutableParams();

        SuspendableContainer raw = null;

        if (ip.getContainerType() == ImmutableParams.ContainerType.FCC) {

            Compressor comp = null;
            switch (ip.getCompression()) {
                case LZ4:
                    comp = new LZ4Compressor();
                    break;
                default:
                    comp = new NoCompressor();
                    break;
            }

            raw = new FastCompressionContainer(
                    containerPath.toString(),
                    ip.getMacroBlockSize(),
                    ip.getBlockSize(),
                    comp,
                    ip.getContainerSpare(),
                    mp.getMacroBlockBufferSize(),
                    mp.isUseDirectIO(),
                    mp.isUseBlockBuffer());

            //		final FastCompressionContainer fcc = new FastCompressionContainer(containerPath.toString(),
            //			4*blockSize, blockSize, new NoCompressor(), this.metaData.getTreeConfig().getContainerSpare());
        } else if (ip.getContainerType() == ImmutableParams.ContainerType.BLOCK) {
            Path dir = containerPath.getParent();
            String name = containerPath.getFileName().toString();
            try {
                raw = new BFWrapper(new FCBlockFileContainer(dir, name, ip.getBlockSize(), mp.isUseDirectIO()));
            } catch (IOException e) {
                throw new DBRuntimeException("Could not create container.", e);
            }
        }
        return new SuspendableConverterContainer(raw, treeNodeConverter, SerializationMode.UNSAFE,
                this.metaData.getImmutableParams().getBlockSize());
    }

    /**
     * Loads a cleanly shut down TAB+-Tree
     *
     * @param metaDataFile
     *            The location of the tree's meta data.
     * @throws IOException
     *             On any error loading the tree's state
     */
    private void loadClean(final Path metaDataFile, MutableParams cfg) throws IOException {
        log.debug("Loading TAB+-Tree state from {}.", metaDataFile);
        this.metaData = PrimaryIndexMetaData.load(metaDataFile, cfg);
        log.debug("Tree meta data: {}", metaDataFile);

        // TODO: Config params
        final Log<Event> WAL = new NoLog<>();
        final Log<Event> OOLog = new NoLog<>();

        this.tree = loadTree(WAL);
        this.appender = createAppender(this.tree, WAL, OOLog);
    }

    /**
     * Recovers the tree from a diry state (e.g., a missed call to
     * {@link #close()}).
     *
     * @param tmpMetaDataFile
     *            the temporary meta-data file.
     * @throws IOException
     *             On any error loading the tree's state
     */
    private void loadDirty(final Path tmpMetaDataFile, MutableParams cfg) throws RecoveryException, IOException {
        log.info("Recovering TAB+-Tree state from dirty meta data at: {}", tmpMetaDataFile);
        this.metaData = PrimaryIndexMetaData.load(tmpMetaDataFile, cfg);

        log.debug("Tree meta data: {}", this.metaData);

        // TODO: Config params
        final Log<Event> WAL = new NoLog<>();
        final Log<Event> OOLog = new NoLog<>();

        // Execute recovery
        final Pair<FastAggregatedBPlusTree, FastAggregatedAppender<Event, PersistentEvent>> recoveryResult =
                recover(WAL, OOLog);

        this.tree = recoveryResult.getFirst();
        this.appender = recoveryResult.getSecond();

        // Recover timestamps
        if (this.appender.getFirst() != null) {
            this.metaData.setTimestamps(this.appender.getFirst().getT1(), this.appender.getLast().getT1());
            // Recover event count
            this.metaData.setEventCount(computeEventCount());
        } else {
            this.metaData.setEventCount(0);
            this.metaData.setTimestamps(Long.MAX_VALUE, Long.MIN_VALUE);
        }

    }

    /**
     * Load a clean tres
     *
     * @param wal
     *            the WAL to use
     * @return the loaded tree
     */
    private FastAggregatedBPlusTree loadTree(final Log<Event> wal) {
        // Create a tree
        final FastAggregatedBPlusTree tree = new FastAggregatedBPlusTree(metaData.getSchema(),
                metaData.getTimeRepresentation(), this.metaData.getImmutableParams().getBlockSize(), true, wal);

        // Loads the container
        final Path path = this.storageDirectory.resolve(CONTAINER_FILE);
        container = loadContainer(path, tree.nodeConverter());

        initializeEmptyTree(tree, container);

        IndexEntry ie = null;
        LongKeyRange rootDescriptor = null;

        if (this.metaData.getTreeRootId() >= 0) {
            // Reconstructs the root entry
            rootDescriptor = new LongKeyRange(this.metaData.getMinTimestamp(), this.metaData.getMaxTimestamp());
            ie = tree.createIndexEntry(this.metaData.getTreeRootParentLevel());
            ie.initialize(this.metaData.getTreeRootId(), new LongSeparator(this.metaData.getMaxTimestamp()));
        }

        // Reconstructs the tree
        tree.initialize(ie, rootDescriptor, new AbstractFunction<Event, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long invoke(final Event argument) {
                        return PrimaryIndexImpl.KEY_FUNCTION.apply(argument);
                    }
                }, container, KEY_CONVERTER,
                SchemaManager.getDataConverter(metaData.getSchema(), metaData.getTimeRepresentation()),
                LongSeparator.FACTORY_FUNCTION, LongKeyRange.FACTORY_FUNCTION);
        return tree;
    }

    ///////////////////////////////////////////////////
    //
    // Appender
    //
    ///////////////////////////////////////////////////

    /**
     * Open an existing container
     *
     * @param containerPath
     *            the path to the container file
     * @param treeNodeConverter
     *            the tree node converter
     * @return the container
     */
    protected SuspendableContainer loadContainer(final Path containerPath, final Converter<?> treeNodeConverter) {
        MutableParams mp = this.metaData.getMutableParams();
        ImmutableParams ip = this.metaData.getImmutableParams();

        SuspendableContainer raw = null;
        if (ip.getContainerType() == ImmutableParams.ContainerType.FCC) {
            raw = new FastCompressionContainer(
                    containerPath.toString(),
                    mp.getMacroBlockBufferSize(),
                    mp.isUseDirectIO(),
                    mp.isUseBlockBuffer());
        } else if (ip.getContainerType() == ImmutableParams.ContainerType.BLOCK) {
            Path dir = containerPath.getParent();
            String name = containerPath.getFileName().toString();
            try {
                raw = new BFWrapper(new FCBlockFileContainer(dir, name, mp.isUseDirectIO()));
            } catch (IOException e) {
                throw new DBRuntimeException("Could not create container.", e);
            }
        }
        return new SuspendableConverterContainer(raw, treeNodeConverter, SerializationMode.UNSAFE,
                this.metaData.getImmutableParams().getBlockSize());
    }

    ///////////////////////////////////////////////////
    //
    // Recovery
    //
    ///////////////////////////////////////////////////

    /**
     * Create a new appender for the given tree
     *
     * @param tree
     *            the tree backing the created appender
     * @param wal
     *            the WAL to use
     * @param ooLog
     *            the out-of-order mirror log
     * @return an appender for the given tree
     */
    private FastAggregatedAppender<Event, PersistentEvent> createAppender(final FastAggregatedBPlusTree tree,
                                                                          final Log<Event> wal,
                                                                          final Log<Event> ooLog) {

        ImmutableParams ip = this.metaData.getImmutableParams();
        MutableParams mp = this.metaData.getMutableParams();

        return new FastAggregatedAppender<>(tree,
                mp.getLeafBufferNodes(), wal, ooLog,
                ip.getTreeSpare(),
                mp.getOutOfOrderQueueSize(),
                mp.getTreeBufferSize(), KEY_FUNCTION, GENERATE_OUT);
    }

    /**
     * Recovers a tree from a dirty state.
     *
     * @param wal
     *            the WAL to use
     * @param ooLog
     *            the out-of-order mirror log
     * @throws DBException
     */
    private Pair<FastAggregatedBPlusTree, FastAggregatedAppender<Event, PersistentEvent>> recover(final Log<Event> wal,
                                                                                                  final Log<Event> ooLog) throws RecoveryException {

        // Create a tree
        final FastAggregatedBPlusTree tree =
                new FastAggregatedBPlusTree(metaData.getSchema(), metaData.getTimeRepresentation(),
                        this.metaData.getImmutableParams().getBlockSize(), true, wal);

        // Load the container
        final Path path = this.storageDirectory.resolve(CONTAINER_FILE);
        container = loadContainer(path, tree.nodeConverter());

        // Initialize as empty
        initializeEmptyTree(tree, container);

        // Recover if container holds data
        PrimaryIndexRecovery.RecoveryResult recoveryResult;
        try {
            recoveryResult = PrimaryIndexRecovery.recoverTreePath(KEY_FUNCTION, container);
        } catch (IllegalStateException e) {
            log.error("Recovery failed. Falling back to brute force.", e);
            recoveryResult = PrimaryIndexRecovery.recoverTreePathWithBruceForce(KEY_FUNCTION, container);
        }


        switch (recoveryResult.getType()) {
            case EMPTY:
                log.debug("Container does not contain any data. Returning fresh tree.");
                break;
            case REGULAR_LOAD:
                log.debug("No recovery necessary, performing regular load.");
                break;
            case RECOVER: {
                SortedMap<Integer, PrimaryIndexRecovery.RNode> exportPath = recoveryResult.getTreePath();
                final BPlusLink.Node rightMostLeaf = exportPath.get(PrimaryIndexRecovery.LEAF_LEVEL).node;
                final Event last = (Event) rightMostLeaf.getLast();
                final Event first = getFirstEvent(container);
                final List<Pair<Long, ? extends Node>> appenderPath = new ArrayList<>();
                exportPath.entrySet().forEach(x -> appenderPath.add(new Pair<>(x.getValue().getId(), x.getValue().getNode())));
                final FastAggregatedAppender<Event, PersistentEvent> recoveryAppender =
                        new FastAggregatedAppender<>(tree, this.metaData.getImmutableParams().getTreeSpare(),
                                this.metaData.getMutableParams().getLeafBufferNodes(), GENERATE_OUT, appenderPath,
                                first, last);

                recoveryAppender.recover();
                recoveryAppender.flushPathAndClose();
            }
        }
        return new Pair<>(tree, createAppender(tree, wal, ooLog));
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public double getCompressionRatio() {
        return container.getCompressionRatio();
    }

    /**
     * Retrieves the first event of the tree
     *
     * @param node
     *            a node on the leaf-level
     * @return the first event inside the tree of the given node
     * @throws DBException
     */
    private Event getFirstEvent(final Container con) throws RecoveryException {
        @SuppressWarnings("unchecked")
        Iterator<Long> ids = con.ids();
        while (ids.hasNext()) {
            Node node = (Node) con.get(ids.next());
            if (node.level == 0 && node.previousNeighbor() == null)
                return (Event) node.getFirst();
        }
        throw new RecoveryException("Could not recover first entry. Leftmost leaf not found.");
    }

    public FastAggregatedBPlusTree getTree() {
        return tree;
    }

    public FastAggregatedAppender<Event, PersistentEvent> getAppender() {
        return appender;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public long estimateIO(long numPages) {
        return (long) (numPages * metaData.getImmutableParams().getBlockSize());//*getCompressionRatio());
    }

    @SuppressWarnings("unchecked")
    public void walkLevel(int level, Consumer<HistogramAccess> consumer) {
        ensureState(State.OPEN);
        var p = appender.getTempRoot(true);

        IndexEntry entry = (IndexEntry) p.getElement1();
        FastAggregatedBPlusTree.Node node = (FastAggregatedBPlusTree.Node) p.getElement2();

        if (level > p.getElement1().level() || level < 1)
            throw new IllegalArgumentException(
                    "Can only walk tree-levels between " + node.level + " (tree height) and 1");

        // Down to first node of level
        while (node.level > level) {
            IndexEntry tmp = (IndexEntry) node.getEntry(0);
            entry.unfix();
            entry = tmp;
            node = entry.get(false);
        }

        Iterator<IndexEntry> entryIter = node.entries();

        AtomicLong minTS = new AtomicLong(metaData.getMinTimestamp());
        AtomicLong counter = new AtomicLong(0);
        while (entryIter.hasNext() || node.nextNeighbor != null) {
            if (entryIter.hasNext()) {
                final IndexEntry current = entryIter.next();
                final Long maxTS = (Long) current.separator().sepValue();
                final TimeInterval interval = new TimeInterval(minTS.get(), maxTS);
                minTS.set(maxTS + 1);

                consumer.accept(new HistogramAccess() {

                    final long index = counter.getAndIncrement();

                    public long getIndex() {
                        return index;
                    }

                    @Override
                    public Aggregation getAggregates(String attribute) {
                        return current.getAggregation(attribute);
                    }

                    @Override
                    public TimeInterval getCoveredTimeInterval() {
                        return interval;
                    }

                    @Override
                    public boolean intersects(List<? extends AttributeRange<? extends Number>> ranges) {
                        return intersectsAll(current, ranges);
                    }

                    @Override
                    public long getEventCount() {
                        for (var a : metaData.getSchema())
                            if (a.getProperty("index") != null && a.getProperty("index").equalsIgnoreCase("true"))
                                return current.getAggregation(a.getName()).getValues()[3].longValue();
                        throw new SchemaException("No attribute is indexed, cannot count events.");
                    }

                    @Override
                    public boolean intersects(AttributeRange<? extends Number> range) {
                        return intersectsSingle(current, range);
                    }

                    /**
                     * @{inheritDoc}
                     */
                    @Override
                    public String toString() {
                        return current.plot();
                    }


                });
            } else {
                entry.unfix();
                entry = (IndexEntry) node.nextNeighbor;
                node = entry.get(false);
                entryIter = node.entries();
            }
        }
    }

    public static enum State {
        OPENING, RECOVERING, OPEN, CLOSED, ERROR
    }

    static class BFWrapper extends AbstractContainer implements SuspendableContainer {
        final FCBlockFileContainer src;

        public BFWrapper(FCBlockFileContainer src) {
            this.src = src;
        }

        private Object convertIdIn(Object id) {
            return ((Long) id) * src.blockSize();
        }

        private Object convertIdOut(Object id) {
            return ((Long) id) / src.blockSize();
        }

        @Override
        public Object get(Object id, boolean unfix) throws NoSuchElementException {
            return src.get(convertIdIn(id));
        }

        @Override
        public Iterator ids() {
            Iterator src = this.src.ids();
            return new Iterator() {
                @Override
                public boolean hasNext() {
                    return src.hasNext();
                }

                @Override
                public Object next() {
                    return convertIdOut(src.next());
                }
            };
        }

        @Override
        public void close() {
            super.close();
            src.close();
        }

        @Override
        public boolean isUsed(Object id) {
            return src.isUsed(convertIdIn(id));
        }

        @Override
        public FixedSizeConverter objectIdConverter() {
            return src.objectIdConverter();
        }

        @Override
        public void remove(Object id) throws NoSuchElementException {
            src.remove(convertIdIn(id));
        }

        @Override
        public Object reserve(xxl.core.functions.Function getObject) {
            return convertIdOut(src.reserve(getObject));
        }

        @Override
        public int size() {
            return src.size();
        }

        @Override
        public void update(Object id, Object object, boolean unfix) throws NoSuchElementException {
            src.update(convertIdIn(id), object, unfix);
        }

        @Override
        public void suspend() {
        }

        @Override
        public void resume() {
        }

        @Override
        public boolean isSuspended() {
            return false;
        }

        @Override
        public Iterator<?> idsBackwards() {
            Deque result = new ArrayDeque(size());
            ids().forEachRemaining(result::addFirst);
            return result.iterator();
        }

        @Override
        public double getCompressionRatio() {
            return 1;
        }

        @Override
        public long getAllocatedSpace() {
            return (long) size() * src.blockSize();
        }

        @Override
        public void allocateSpace(long space) {
        }
    }
}
