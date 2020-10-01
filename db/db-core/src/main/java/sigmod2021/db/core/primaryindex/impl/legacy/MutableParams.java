package sigmod2021.db.core.primaryindex.impl.legacy;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 */
public class MutableParams {

    private static final byte VERSION_1 = 1;

    public static final byte CURRENT_VERSION = VERSION_1;

    //////////////////////////////////////////////////////////////////////////
    //
    // Appender Stuff
    //
    //////////////////////////////////////////////////////////////////////////

    /**
     * The size of the outOfOrderQueue (in slots)
     */
    private int outOfOrderQueueSize = 200;

    /**
     * The size of the tree buffer (in slots)
     */
    private int treeBufferSize = 400;

    /**
     * The size of the buffers of BPlusAppenders. Sizes between 10 and 100
     * worked fine in our experiments.
     */
    private int leafBufferNodes = 1;

    //////////////////////////////////////////////////////////////////////////
    //
    // Container Stuff
    //
    //////////////////////////////////////////////////////////////////////////

    /**
     * The number of macro blocks to buffer
     */
    private int macroBlockBufferSize = 200;

    /**
     * Tells whether to use O_DIRECT for file-access, bypassing the OS's page
     * buffer and IO-scheduler. Currently available in Linux only. Should only
     * be used for measurements as performance will degrade in most cases.
     *
     */
    private boolean useDirectIO = false;

    /**
     * Use the container's interal block buffer to speed up sequential reads.
     */
    private boolean useBlockBuffer = true;

    public static MutableParams load(final DataInput in) throws IOException {
        // Read version
        /*byte version = */
        in.readByte();

        final MutableParams result = new MutableParams();
        // This is for version 1 -- apply a switch on version increase
        result.outOfOrderQueueSize = in.readInt();
        result.treeBufferSize = in.readInt();
        result.leafBufferNodes = in.readInt();
        result.macroBlockBufferSize = in.readInt();
        result.useDirectIO = in.readBoolean();
        result.useBlockBuffer = in.readBoolean();

        return result;
    }

    /**
     * @return the outOfOrderQueueSize
     */
    public int getOutOfOrderQueueSize() {
        return this.outOfOrderQueueSize;
    }

    /**
     * @param outOfOrderQueueSize the outOfOrderQueueSize to set
     */
    public void setOutOfOrderQueueSize(int outOfOrderQueueSize) {
        this.outOfOrderQueueSize = outOfOrderQueueSize;
    }

    /**
     * @return the treeBufferSize
     */
    public int getTreeBufferSize() {
        return this.treeBufferSize;
    }

    /**
     * @param treeBufferSize the treeBufferSize to set
     */
    public void setTreeBufferSize(int treeBufferSize) {
        this.treeBufferSize = treeBufferSize;
    }

    /**
     * @return the leafBufferNodes
     */
    public int getLeafBufferNodes() {
        return this.leafBufferNodes;
    }

    /**
     * @param leafBufferNodes the leafBufferNodes to set
     */
    public void setLeafBufferNodes(int leafBufferNodes) {
        this.leafBufferNodes = leafBufferNodes;
    }

    /**
     * @return the macroBlockBufferSize
     */
    public int getMacroBlockBufferSize() {
        return this.macroBlockBufferSize;
    }

    /**
     * @param macroBlockBufferSize the macroBlockBufferSize to set
     */
    public void setMacroBlockBufferSize(int macroBlockBufferSize) {
        this.macroBlockBufferSize = macroBlockBufferSize;
    }

    /**
     * @return the useDirectIO
     */
    public boolean isUseDirectIO() {
        return this.useDirectIO;
    }

    /**
     * @param useDirectIO the useDirectIO to set
     */
    public void setUseDirectIO(boolean useDirectIO) {
        this.useDirectIO = useDirectIO;
    }

    /**
     * @return the useBlockBuffer
     */
    public boolean isUseBlockBuffer() {
        return this.useBlockBuffer;
    }

    /**
     * @param useBlockBuffer the useBlockBuffer to set
     */
    public void setUseBlockBuffer(boolean useBlockBuffer) {
        this.useBlockBuffer = useBlockBuffer;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public String toString() {
        return String.format(
                "MutableParams [%n  outOfOrderQueueSize=%s%n  treeBufferSize=%s%n  leafBufferNodes=%s%n  macroBlockBufferSize=%s%n  useDirectIO=%s%n  useBlockBuffer=%s]",
                this.outOfOrderQueueSize, this.treeBufferSize, this.leafBufferNodes, this.macroBlockBufferSize,
                this.useDirectIO, this.useBlockBuffer);
    }

    public void persist(final DataOutput dout) throws IOException {
        // Write version
        dout.writeByte(CURRENT_VERSION);
        dout.writeInt(this.outOfOrderQueueSize);
        dout.writeInt(this.treeBufferSize);
        dout.writeInt(this.leafBufferNodes);
        dout.writeInt(this.macroBlockBufferSize);
        dout.writeBoolean(this.useDirectIO);
        dout.writeBoolean(this.useBlockBuffer);
    }

}
