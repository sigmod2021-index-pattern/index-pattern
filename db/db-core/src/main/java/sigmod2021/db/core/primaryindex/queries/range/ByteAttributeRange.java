package sigmod2021.db.core.primaryindex.queries.range;

import sigmod2021.event.Attribute.DataType;

/**
 */
public class ByteAttributeRange extends AttributeRange<Byte> {

    /**
     * Creates a new ByteAttributeRange instance
     *
     * @param name
     *            the attribute name
     * @param lower
     *            the lower bound (inclusive)
     * @param upper
     *            the upper bound (inclusive)
     */
    public ByteAttributeRange(final String name, final Byte lower, final Byte upper, boolean lowerInclusive, boolean upperInclusive) {
        super(name, DataType.BYTE, Byte.class, lower, upper, lowerInclusive, upperInclusive);
    }

}
