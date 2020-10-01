package sigmod2021.esp.api.epa;

import sigmod2021.esp.api.expression.BooleanExpression;
import sigmod2021.event.EventSchema;


/**
 * Representation of a Filter-Agent.
 * Only forwards events, satisfying the given condition.
 */
public class Filter implements EPA {

    private static final long serialVersionUID = 1L;
    /**
     * The condition to check on each event
     */
    private final BooleanExpression condition;
    /**
     * The input-EPA of this filter
     */
    private EPA input;

    /**
     * Constructs a new instance
     *
     * @param input     the input EPA
     * @param condition the filter-condition
     */
    public Filter(EPA input, BooleanExpression condition) {
        this.input = input;
        this.condition = condition;
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
     * @return the filter-condition
     */
    public BooleanExpression getCondition() {
        return condition;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public EventSchema computeOutputSchema(EventSchema... inputSchemas) {
        return inputSchemas[0];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "Filter [input=" + input + ", condition=" + condition + "]";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((condition == null) ? 0 : condition.hashCode());
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
        Filter other = (Filter) obj;
        if (condition == null) {
            if (other.condition != null)
                return false;
        } else if (!condition.equals(other.condition))
            return false;
        if (input == null) {
            if (other.input != null)
                return false;
        } else if (!input.equals(other.input))
            return false;
        return true;
    }
}
