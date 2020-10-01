package sigmod2021.esp.api.epa.window;

import sigmod2021.esp.api.epa.EPA;

import java.util.Arrays;

/**
 * A partitioned count-based window.
 */
public class PartitionedCountWindow extends CountWindow {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * Attributes by which the input event stream is partitioned.
     */
    private final String[] partitionAttributes;

    /**
     * @param input               The input EPA of this window
     * @param size                The size of this window
     * @param partitionAttributes the attributes to partition the input stream with
     */
    public PartitionedCountWindow(EPA input, long size, String... partitionAttributes) {
        this(input, size, 1L, partitionAttributes);
    }

    /**
     * @param input               The input EPA of this window
     * @param size                The size of this window
     * @param jump                The jump-size of this window
     * @param partitionAttributes the attributes to partition the input stream with
     */
    public PartitionedCountWindow(EPA input, long size, long jump, String... partitionAttributes) {
        super(input, size, jump);
        this.partitionAttributes = new String[partitionAttributes.length];
        System.arraycopy(partitionAttributes, 0, this.partitionAttributes, 0, partitionAttributes.length);
    }

    /**
     * @return the partitioning attribute names
     */
    public String[] getPartitionAttributes() {
        return partitionAttributes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + Arrays.hashCode(partitionAttributes);
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        PartitionedCountWindow other = (PartitionedCountWindow) obj;
        if (!Arrays.equals(partitionAttributes, other.partitionAttributes))
            return false;
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "PartitionedCountWindow [partitionAttributes=" + Arrays.toString(partitionAttributes) + ", input=" + input + ", size=" + size + ", jump=" + slide + "]";
    }
}
