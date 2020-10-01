package sigmod2021.db.core.primaryindex.queries.range;

import sigmod2021.event.Attribute.DataType;

/**
 */
public class DoubleAttributeRange extends AttributeRange<Double> {

    /**
     * Creates a new DoubleAttributeRange instance
     *
     * @param name
     *            the attribute name
     * @param lower
     *            the lower bound (inclusive)
     * @param upper
     *            the upper bound (inclusive)
     */
    public DoubleAttributeRange(final String name, final Double lower, final Double upper, boolean lowerInclusive, boolean upperInclusive) {
        super(name, DataType.DOUBLE, Double.class, lower, upper, lowerInclusive, upperInclusive);
    }
}
