package sigmod2021.db.core.primaryindex.queries.range;

import sigmod2021.event.Attribute;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.Event;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

/**
 *
 */
public abstract class AttributeRange<T extends Comparable<T>> {

    private final String name;
    private final DataType dataType;
    private final Class<T> clazz;
    private final T lower;
    private final T upper;
    private final boolean lowerInclusive;
    private final boolean upperInclusive;

    /**
     * Creates a new AttributeRange instance
     *
     * @param name
     * @param dataType
     * @param lower
     * @param upper
     */
    public AttributeRange(final String name, final DataType dataType, final Class<T> clazz, final T lower,
                          final T upper, boolean lowerInclusive, boolean upperInclusive) {
        super();
        this.name = name;
        this.dataType = dataType;
        this.clazz = clazz;
        this.lower = lower;
        this.upper = upper;
        this.lowerInclusive = lowerInclusive;
        this.upperInclusive = upperInclusive;
    }

    /**
     * @return the name
     */
    public String getName() {
        return this.name;
    }

    /**
     * @return the dataType
     */
    public DataType getDataType() {
        return this.dataType;
    }

    /**
     * @return the lower
     */
    public T getLower() {
        return this.lower;
    }

    /**
     * @return the upper
     */
    public T getUpper() {
        return this.upper;
    }

    /**
     * @return the lowerInclusive
     */
    public boolean isLowerInclusive() {
        return this.lowerInclusive;
    }

    /**
     * @return the upperInclusive
     */
    public boolean isUpperInclusive() {
        return this.upperInclusive;
    }

    public EventMatcher getMatcher(final EventSchema schema) throws SchemaException {
        final Attribute attr = schema.byName(this.name);
        if (attr.getType() != this.dataType)
            throw new SchemaException(String.format(
                    "Incompatible data type for attribute \"%s\". Schema type is %s, but range type is %s",
                    attr.getName(), attr.getType(), this.dataType));

        final int index = schema.getAttributeIndex(this.name);

        return new EventMatcher() {
            @Override
            public boolean matches(Event event) {
                return contains(event.get(index, clazz));
            }
        };
    }

    /**
     * @param value
     * @return
     */
    public boolean contains(final T value) {
        return this.lower.compareTo(value) <= 0 && this.upper.compareTo(value) >= 0;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public String toString() {
        return String.format("%s:%s [%s, %s]", this.name, this.dataType, this.lower, this.upper);
    }

    public boolean intersects(AttributeRange<? extends T> other) {
        int cmpLower = lower.compareTo(other.upper);

        if (cmpLower > 0)
            return false;
        else if (cmpLower == 0 && !(lowerInclusive && other.upperInclusive))
            return false;

        int cmpUpper = upper.compareTo(other.lower);

        if (cmpUpper < 0)
            return false;
        else if (cmpUpper == 0 && !(upperInclusive && other.lowerInclusive))
            return false;

        return true;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.dataType == null) ? 0 : this.dataType.hashCode());
        result = prime * result + ((this.lower == null) ? 0 : this.lower.hashCode());
        result = prime * result + ((this.name == null) ? 0 : this.name.hashCode());
        result = prime * result + ((this.upper == null) ? 0 : this.upper.hashCode());
        return result;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final AttributeRange<?> other = (AttributeRange<?>) obj;
        if (this.dataType != other.dataType)
            return false;
        if (this.lower == null) {
            if (other.lower != null)
                return false;
        } else if (!this.lower.equals(other.lower))
            return false;
        if (this.name == null) {
            if (other.name != null)
                return false;
        } else if (!this.name.equals(other.name))
            return false;
        if (this.upper == null) {
            if (other.upper != null)
                return false;
        } else if (!this.upper.equals(other.upper))
            return false;
        return true;
    }

    public interface EventMatcher {
        boolean matches(Event event);
    }
}
