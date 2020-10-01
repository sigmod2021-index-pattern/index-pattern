package sigmod2021.db.core.primaryindex.queries.range;

import sigmod2021.event.Attribute.DataType;

/**
 */
public class FloatAttributeRange extends AttributeRange<Float> {

    /**
     * Creates a new FloatAttributeRange instance
     *
     * @param name
     *            the attribute name
     * @param lower
     *            the lower bound (inclusive)
     * @param upper
     *            the upper bound (inclusive)
     */
    public FloatAttributeRange(final String name, final Float lower, final Float upper, boolean lowerInclusive, boolean upperInclusive) {
        super(name, DataType.FLOAT, Float.class, lower, upper, lowerInclusive, upperInclusive);
    }

}
