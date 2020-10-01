package sigmod2021.db.core.primaryindex.impl.legacy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sigmod2021.event.Event;
import sigmod2021.event.EventSchema;
import sigmod2021.event.TimeRepresentation;
import xxl.core.indexStructures.BPlusTree;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 *
 */
public class PrimaryIndexMetaData {

    private static final Logger log = LoggerFactory.getLogger(PrimaryIndexMetaData.class);

    private static final byte VERSION_1 = 1;

    public static final byte CURRENT_VERSION = VERSION_1;

    /** The schema of stored events */
    private final EventSchema schema;

    /** The time representation to use */
    private final TimeRepresentation timeRepresentation;

    private final ImmutableParams immutableParams;

    private MutableParams mutableParams;

    private long minTimestamp;

    private long maxTimestamp;

    private long eventCount;

    private int treeRootParentLevel;

    private long treeRootId;

    /**
     * @param schema
     * @param timeRepresentation
     */
    public PrimaryIndexMetaData(final EventSchema schema, final TimeRepresentation timeRepresentation,
                                final ImmutableParams ip, final MutableParams mp) {
        this.schema = schema;
        this.timeRepresentation = timeRepresentation;
        this.immutableParams = ip;
        this.mutableParams = mp;
        this.minTimestamp = Long.MAX_VALUE;
        this.maxTimestamp = Long.MIN_VALUE;
        this.treeRootParentLevel = 0;
        this.treeRootId = -1;
    }

    public static PrimaryIndexMetaData load(final Path metaDataFile) throws IOException {
        return load(metaDataFile, null);
    }

    /**
     * Loads the metadata from the given directory
     *
     * @param metaDataFile
     *            the file to load the meta data from
     * @return the loaded meta data
     * @throws IOException
     *             on any error loading the meta-data
     */
    public static PrimaryIndexMetaData load(final Path metaDataFile, final MutableParams override) throws IOException {
        log.debug("Loading TABMetaData from {}", metaDataFile);

        try (DataInputStream din = new DataInputStream(Files.newInputStream(metaDataFile, StandardOpenOption.READ))) {
            // Read version
            /*byte version = */
            din.readByte();

            // This is for version 1 -- apply a switch on version increase
            // Read schema and representation
            final PrimaryIndexMetaData result = new PrimaryIndexMetaData(EventSchema.read(din), TimeRepresentation.valueOf(din.readUTF()),
                    ImmutableParams.load(din), MutableParams.load(din));
            // Read timestamps
            result.minTimestamp = din.readLong();
            result.maxTimestamp = din.readLong();
            result.eventCount = din.readLong();
            // Read tree root
            result.treeRootId = din.readLong();
            result.treeRootParentLevel = din.readInt();

            if (override != null) {
                log.debug("Overriding mutable config parameters.");
                result.mutableParams = override;
            }

            log.debug("Finished loading TABMetaData from {}", metaDataFile);
            return result;
        }
    }

    /**
     * @param event
     *            the event to check
     * @return <tt>true</tt> if this event is out of order, <tt>false</tt>
     *         otherwise
     */
    public boolean isOutOfOrder(final Event event) {
        return event.getT1() < this.maxTimestamp;
    }

    /**
     * Updates the timestamp boundaries
     *
     * @param event
     *            the event to insert
     * @return <tt>true</tt> if this event is out of order, <tt>false</tt>
     *         otherwise
     */
    public void updateTimestamps(final Event event) {
        if (event.getT1() >= this.maxTimestamp) {
            this.maxTimestamp = event.getT1();
        }
        if (event.getT1() < this.minTimestamp) {
            this.minTimestamp = event.getT1();
        }
    }

    public void setTimestamps(long min, long max) {
        this.minTimestamp = min;
        this.maxTimestamp = max;
    }

    public void increaseEventCount() {
        eventCount++;
    }

    /**
     * Stores the current root of the tree
     *
     * @param root
     *            the tree root
     */
    public void setTreeRoot(final BPlusTree.IndexEntry root) {
        if (root != null) {
            setTreeRoot((Long) root.id(), root.parentLevel());
        } else {
            setTreeRoot(-1L, 0);
        }
    }

    /**
     * Stores the current root of the tree
     *
     * @param id
     *            the root node's id
     * @param parentLevel
     *            the root's parent level
     */
    private void setTreeRoot(final long id, final int parentLevel) {
        this.treeRootId = id;
        this.treeRootParentLevel = parentLevel;
    }

    /**
     * @return the schema
     */
    public EventSchema getSchema() {
        return this.schema;
    }

    /**
     * @return the timeRepresentation
     */
    public TimeRepresentation getTimeRepresentation() {
        return this.timeRepresentation;
    }

    /**
     * @return the minTimestamp
     */
    public long getMinTimestamp() {
        return this.minTimestamp;
    }

    /**
     * @return the maxTimestamp
     */
    public long getMaxTimestamp() {
        return this.maxTimestamp;
    }

    /**
     * @return the number of events stored in this tree
     */
    public long getEventCount() {
        return this.eventCount;
    }

    public void setEventCount(long value) {
        this.eventCount = value;
    }

    /**
     * @return the treeRootParentLevel
     */
    public int getTreeRootParentLevel() {
        return this.treeRootParentLevel;
    }

    /**
     * @return the treeRootId
     */
    public long getTreeRootId() {
        return this.treeRootId;
    }

    /**
     * @return the mutableParams
     */
    public MutableParams getMutableParams() {
        return this.mutableParams;
    }

    /**
     * @param mutableParams
     *            the mutableParams to set
     */
    public void setMutableParams(MutableParams mutableParams) {
        this.mutableParams = mutableParams;
    }

    /**
     * @return the immutableParams
     */
    public ImmutableParams getImmutableParams() {
        return this.immutableParams;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public String toString() {
        return String.format(
                "MetaData [schema: %s, timeRepresentation: %s, immutableParams: %s, mutableParams: %s, minTimestamp: %s, maxTimestamp: %s, treeRootParentLevel: %s, treeRootId: %s]",
                this.schema, this.timeRepresentation, this.immutableParams, this.mutableParams, this.minTimestamp,
                this.maxTimestamp, this.treeRootParentLevel, this.treeRootId);
    }

    /**
     * Stores this meta-data instance to the given directory.
     *
     * @param file the
     *            file to store the meta data at
     * @throws IOException
     *             on any error saving the meta-data
     */
    public void persist(final Path file) throws IOException {
        log.debug("Writing MetaData to {}", file);

        try (DataOutputStream dout = new DataOutputStream(
                Files.newOutputStream(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE))) {
            // Write version
            dout.writeByte(CURRENT_VERSION);
            // Write schema
            this.schema.write(dout);
            // Write representation
            dout.writeUTF(this.timeRepresentation.name());
            // Write aux config
            this.immutableParams.persist(dout);
            this.mutableParams.persist(dout);
            // Write time bounds
            dout.writeLong(this.minTimestamp);
            dout.writeLong(this.maxTimestamp);
            dout.writeLong(eventCount);
            // Write tree root
            dout.writeLong(this.treeRootId);
            dout.writeInt(this.treeRootParentLevel);

            log.debug("Finished writing TABMetaData to {}", file);
        }
    }
}
