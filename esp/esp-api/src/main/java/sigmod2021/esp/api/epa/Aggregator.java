package sigmod2021.esp.api.epa;

import sigmod2021.esp.api.epa.aggregates.*;
import sigmod2021.esp.api.epa.aggregates.spatial.TemporalLineStringMerge;
import sigmod2021.esp.api.epa.aggregates.spatial.Trajectory;
import sigmod2021.event.Attribute;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Representation of an Aggregator agent. Computes the given aggregates
 * based on the temporal validity of incoming events.
 * <p>
 */
public class Aggregator implements EPA, Iterable<Aggregate> {

    private static final long serialVersionUID = 1L;
    /**
     * The list of aggregates to compute
     */
    private final List<Aggregate> aggregates;
    /**
     * The input-EPA of this filter
     */
    private EPA input;


    /**
     * Constructs a new instance
     *
     * @param input      the input-EPA
     * @param aggregates the aggregates to compute
     */
    public Aggregator(EPA input, Aggregate... aggregates) {
        if (aggregates.length < 1)
            throw new IllegalArgumentException("An aggregator requires at least one aggregate");
        this.input = input;
        this.aggregates = Arrays.asList(aggregates);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public EPA[] getInputEPAs() {
        return new EPA[]{input};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setInput(int idx, EPA input) {
        if (idx == 0)
            this.input = input;
        else
            throw new IllegalArgumentException("Invalid input index: " + idx);
    }

    /**
     * @return the aggregates to compute
     */
    public List<Aggregate> getAggregates() {
        return aggregates;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Aggregate> iterator() {
        return aggregates.iterator();
    }

    /**
     * {@inheritDoc}
     *
     * @throws SchemaException
     */
    @Override
    public EventSchema computeOutputSchema(EventSchema... inputSchemas) throws SchemaException {
        EventSchema schemaIn = inputSchemas[0];
        Attribute[] schema = new Attribute[aggregates.size()];
        int i = 0; // attribute array pointer
        DataType attrType = DataType.STRING;
        for (Aggregate agg : aggregates) {
            if (agg instanceof Group || agg instanceof Maximum || agg instanceof Minimum || agg instanceof Last || agg instanceof First)
                attrType = schemaIn.byName(agg.getAttributeIn()).getType();
            else if (agg instanceof Average || agg instanceof Stddev)
                attrType = DataType.DOUBLE;
            else if (agg instanceof Count)
                attrType = DataType.LONG;
            else if (agg instanceof Trajectory || agg instanceof TemporalLineStringMerge)
                attrType = DataType.GEOMETRY;
            else if (agg instanceof Sum) {
                switch (schemaIn.byName(agg.getAttributeIn()).getType()) {
                    case BYTE:
                    case SHORT:
                    case INTEGER:
                    case LONG:
                        attrType = DataType.LONG;
                        break;
                    case FLOAT:
                    case DOUBLE:
                        attrType = DataType.DOUBLE;
                        break;
                    default:
                        throw new IllegalArgumentException("Cannot compute sum of type: " + schemaIn.byName(agg.getAttributeIn()).getType());
                }
            }
            schema[i++] = new Attribute(agg.getAttributeOut(), attrType);
        }
        return new EventSchema(schema);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "Aggregator [input=" + input + ", aggregates=" + aggregates + "]";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((aggregates == null) ? 0 : aggregates.hashCode());
        result = prime * result + ((input == null) ? 0 : input.hashCode());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Aggregator other = (Aggregator) obj;
        if (aggregates == null) {
            if (other.aggregates != null)
                return false;
        } else if (!aggregates.equals(other.aggregates))
            return false;
        if (input == null) {
            if (other.input != null)
                return false;
        } else if (!input.equals(other.input))
            return false;
        return true;
    }
}
