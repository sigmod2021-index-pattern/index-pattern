package sigmod2021.db.core.primaryindex.impl.legacy;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 */
public class ImmutableParams {

    private static final byte VERSION_1 = 1;

    ;
    private static final byte VERSION_2 = 2;
    private static final byte VERSION_3 = 3;
    public static final byte CURRENT_VERSION = VERSION_3;
    /**
     * Defines the logical size of a memory page on disk.
     */
    private int blockSize = 8192;
    /**
     * The size of a macro block in bytes. Must be a multiple of the
     * {@link #blockSize}
     */
    private int macroBlockSize = 4 * blockSize;
    /**
     * The spare left in each macro of the container (for out-of-order data)
     */
    private float containerSpare = 0.1f;
    /**
     * The spare left in each node of the tree (for out-of-order data)
     */
    private float treeSpare = 0.1f;
    /**
     * The compression to use
     */
    private Compression compression = Compression.LZ4;
    /** The container type to use */
    private ContainerType containerType = ContainerType.FCC;

    public static ImmutableParams load(final DataInput in) throws IOException {
        // Read version
        int version = in.readByte();

        final ImmutableParams result = new ImmutableParams();
        // This is for version 1 -- apply a switch on version increase
        result.blockSize = in.readInt();
        result.containerSpare = in.readFloat();
        result.treeSpare = in.readFloat();
        result.macroBlockSize = in.readInt();

        if (version == VERSION_1)
            result.compression = Compression.LZ4;
        else
            result.compression = Compression.valueOf(in.readUTF());

        if (version == VERSION_3) {
            result.containerType = ContainerType.valueOf(in.readUTF());
        } else {
            result.containerType = ContainerType.FCC;
        }

        return result;
    }

    /**
     * @return the blockSize
     */
    public int getBlockSize() {
        return this.blockSize;
    }

    /**
     * @param blockSize
     *            the blockSize to set
     */
    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    /**
     * @return the macroBlockSize
     */
    public int getMacroBlockSize() {
        return this.macroBlockSize;
    }

    /**
     * @param macroBlockSize
     *            the macroBlockSize to set
     */
    public void setMacroBlockSize(int macroBlockSize) {
        this.macroBlockSize = macroBlockSize;
    }

    /**
     * @return the containerSpare
     */
    public float getContainerSpare() {
        return this.containerSpare;
    }

    /**
     * @param containerSpare
     *            the containerSpare to set
     */
    public void setContainerSpare(float containerSpare) {
        this.containerSpare = containerSpare;
    }

    /**
     * @return the treeSpare
     */
    public float getTreeSpare() {
        return this.treeSpare;
    }

    /**
     * @param treeSpare
     *            the treeSpare to set
     */
    public void setTreeSpare(float treeSpare) {
        this.treeSpare = treeSpare;
    }

    /**
     * @return the compression
     */
    public Compression getCompression() {
        return this.compression;
    }

    /**
     * @param compression the compression to set
     */
    public void setCompression(Compression compression) {
        this.compression = compression;
    }

    public ContainerType getContainerType() {
        return containerType;
    }

    public void setContainerType(ContainerType containerType) {
        this.containerType = containerType;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public String toString() {
        return String.format(
                "ImmutableParams [%n  blockSize=%s%n  macroBlockSize=%s%n  containerSpare=%s%n  treeSpare=%s%n, containerType=%s%n, compression=%s]",
                this.blockSize, this.macroBlockSize, this.containerSpare, this.treeSpare, this.containerType, this.compression);
    }

    public void persist(final DataOutput dout) throws IOException {
        // Write version
        dout.writeByte(CURRENT_VERSION);
        dout.writeInt(this.blockSize);
        dout.writeFloat(this.containerSpare);
        dout.writeFloat(this.treeSpare);
        dout.writeInt(this.macroBlockSize);
        dout.writeUTF(this.compression.name());
        dout.writeUTF(this.containerType.name());
    }

    public static enum Compression {
        NO_COMPRESSION, LZ4
    }

    public static enum ContainerType {
        FCC, BLOCK
    }

}
