package sigmod2021.db.event;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Represents the location of an entry in the secondary time index. The location
 * is composed of the id of the block on the container the entry is stored and
 * the offset within this block.
 */
public class TID implements Comparable<TID> {

    /**
     * The id of the block on the memory container the entry is located in
     */
    private final long blockId;

    /**
     * The offset of the entry within the its block
     */
    private final int offset;

    // =============================================================================================================

    /**
     * Creates a new event id.
     *
     * @param blockId the block of the event
     * @param offset  the event`s offset within the block
     */
    public TID(final long blockId, final int offset) {
        this.blockId = blockId;
        this.offset = offset;
    }

    /**
     * @return the blockId
     */
    public long getBlockId() {
        return this.blockId;
    }

    // =============================================================================================================

    /**
     * @return the offset
     */
    public int getOffset() {
        return this.offset;
    }

    // =============================================================================================================

    @Override
    public int compareTo(final TID o) {
        if (this.blockId == o.blockId)
            return Integer.compare(this.offset, o.offset);
        else
            return Long.compare(this.blockId, o.blockId);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public String toString() {
        return String.format("%d:%d", blockId, offset);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (this.blockId ^ (this.blockId >>> 32));
        result = prime * result + this.offset;
        return result;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TID other = (TID) obj;
        if (this.blockId != other.blockId)
            return false;
        if (this.offset != other.offset)
            return false;
        return true;
    }


}
