package sigmod2021.db.core.secondaryindex;

import xxl.core.io.converters.MeasuredConverter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Represents the location of an entry in the secondary time index.
 * The location is composed of the id of the block on the container
 * the entry is stored and the offset within this block.
 */
public class EventID implements Comparable<EventID> {

    /**
     * The id of the block on the memory container the entry is located in
     */
    private final long blockId;

    /**
     * The offset of the entry within the its block
     */
    private final int offset;

    /**
     * The event`s timestamp. This is required for the fallback solution,
     * i.e. if the event`s containing node has been split/modified
     */
    private final long timestamp;

    /**
     * The absolute position of the corresponding event in the stream.
     */
    private final long sequenceId;

    // =============================================================================================================

//    /**
//     * Creates an empty event id.
//     */
//	public EventID(){}

    /**
     * Creates a new event id.
     *
     * @param blockId    the block of the event
     * @param offset     the event`s offset within the block
     * @param timestamp  the timestamp for fallback lookups
     * @param sequenceId The absolute position of the corresponding event in the stream.
     */
    public EventID(Long blockId, int offset, long timestamp, long sequenceId) {
        this.blockId = blockId;
        this.offset = offset;
        this.timestamp = timestamp;
        this.sequenceId = sequenceId;
    }


    // =============================================================================================================

    public static MeasuredConverter<EventID> getConverter() {
        return new MeasuredConverter<EventID>() {

            /** The serialVersionUID */
            private static final long serialVersionUID = 1L;

            @Override
            public EventID read(DataInput dataInput, EventID object) throws IOException {
                return new EventID(dataInput.readLong(), dataInput.readInt(), dataInput.readLong(), dataInput.readLong());
            }

            @Override
            public void write(DataOutput dataOutput, EventID object) throws IOException {
                dataOutput.writeLong(object.blockId);
                dataOutput.writeInt(object.offset);
                dataOutput.writeLong(object.timestamp);
                dataOutput.writeLong(object.sequenceId);
            }

            @Override
            public int getMaxObjectSize() {
                return 28;
            }

        };
    }

    /**
     * @return the blockId
     */
    public long getBlockId() {
        return this.blockId;
    }

    /**
     * @return the offset
     */
    public int getOffset() {
        return this.offset;
    }

    /**
     * @return the timestamp
     */
    public long getTimestamp() {
        return this.timestamp;
    }

    /**
     * @return the sequenceId
     */
    public long getSequenceId() {
        return this.sequenceId;
    }

    // =============================================================================================================

    @Override
    public String toString() {
        return "blockId: " + blockId + ", offset: " + offset + ", timestamp:" + timestamp + ", sequenceId:" + sequenceId;
    }

    // =============================================================================================================

    @Override
    public int compareTo(EventID o) {
        return Long.compare(sequenceId, o.sequenceId);
//        if (blockId == o.blockId)
//            return offset - o.offset;
//        else
//            return (int)(blockId - o.blockId);
    }
}
