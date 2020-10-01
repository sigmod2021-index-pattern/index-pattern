package sigmod2021.db.core.primaryindex.queries.range;

import sigmod2021.event.Attribute.DataType;

/**
 */
public class IntAttributeRange extends AttributeRange<Integer> {

    /**
     * Creates a new IntAttributeRange instance
     *
     * @param name
     *            the attribute name
     * @param lower
     *            the lower bound (inclusive)
     * @param upper
     *            the upper bound (inclusive)
     */
    public IntAttributeRange(final String name, final Integer lower, final Integer upper, boolean lowerInclusive, boolean upperInclusive) {
        super(name, DataType.INTEGER, Integer.class, lower, upper, lowerInclusive, upperInclusive);
    }

}
