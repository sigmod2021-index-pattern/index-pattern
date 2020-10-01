package sigmod2021.db.core.primaryindex.queries.range;

import sigmod2021.event.Attribute.DataType;

/**
 */
public class ShortAttributeRange extends AttributeRange<Short> {

    /**
     * Creates a new ShortAttributeRange instance
     *
     * @param name
     *            the attribute name
     * @param lower
     *            the lower bound (inclusive)
     * @param upper
     *            the upper bound (inclusive)
     */
    public ShortAttributeRange(final String name, final Short lower, final Short upper, boolean lowerInclusive, boolean upperInclusive) {
        super(name, DataType.SHORT, Short.class, lower, upper, lowerInclusive, upperInclusive);
    }

}
