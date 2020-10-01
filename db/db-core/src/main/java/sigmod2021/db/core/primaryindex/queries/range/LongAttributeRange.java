package sigmod2021.db.core.primaryindex.queries.range;

import sigmod2021.event.Attribute.DataType;

/**
 */
public class LongAttributeRange extends AttributeRange<Long> {

    /**
     * Creates a new LongAttributeRange instance
     *
     * @param name
     *            the attribute name
     * @param lower
     *            the lower bound (inclusive)
     * @param upper
     *            the upper bound (inclusive)
     */
    public LongAttributeRange(final String name, final Long lower, final Long upper, boolean lowerInclusive, boolean upperInclusive) {
        super(name, DataType.LONG, Long.class, lower, upper, lowerInclusive, upperInclusive);
    }

}
