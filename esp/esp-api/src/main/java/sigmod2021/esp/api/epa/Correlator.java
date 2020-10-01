package sigmod2021.esp.api.epa;

import sigmod2021.esp.api.expression.BooleanExpression;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

/**
 * Representation of an Correlator agent. Joins event from the given
 * streams based on their temporal validity and a given join-condition
 */
public class Correlator implements EPA {

    private static final long serialVersionUID = 1L;
    /**
     * A name alias for the first stream
     */
    private final String leftAlias;
    /**
     * A name alias for the second stream
     */
    private final String rightAlias;
    /**
     * The join condition
     */
    private final BooleanExpression condition;
    /**
     * The first input-stream
     */
    private EPA left;
    /**
     * The second input-stream
     */
    private EPA right;


    /**
     * @param left      The first input-stream
     * @param right     The second input-stream
     * @param condition The join condition
     */
    public Correlator(EPA left, EPA right, BooleanExpression condition) {
        this(left, null, right, null, condition);
    }

    /**
     * @param left       The first input-stream
     * @param leftAlias  a name alias for the first input stream. Used to guarantee disjoint schemas of input-streams
     * @param right      The second input-stream
     * @param rightAlias a name alias for the second input stream. Used to guarantee disjoint schemas of input-streams
     * @param condition  The join condition
     */
    public Correlator(EPA left, String leftAlias, EPA right, String rightAlias, BooleanExpression condition) {
        this.left = left;
        this.right = right;
        this.leftAlias = leftAlias;
        this.rightAlias = rightAlias;
        this.condition = condition;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public EPA[] getInputEPAs() {
        return new EPA[]{left, right};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setInput(int idx, EPA input) {
        if (idx == 0)
            this.left = input;
        else if (idx == 1)
            this.right = input;
        else
            throw new IllegalArgumentException("Invalid input index: " + idx);
    }

    /**
     * @return The join condition
     */
    public BooleanExpression getCondition() {
        return condition;
    }

    /**
     * @return The name alias of the first stream
     */
    public String getLeftAlias() {
        return leftAlias;
    }

    /**
     * @return The name alias of the second stream
     */
    public String getRightAlias() {
        return rightAlias;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "Correlator [left=" + left + " AS " + leftAlias + ", right=" + right + " AS " + rightAlias + ", condition=" + condition + "]";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((condition == null) ? 0 : condition.hashCode());
        result = prime * result + ((left == null) ? 0 : left.hashCode());
        result = prime * result + ((right == null) ? 0 : right.hashCode());
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
        Correlator other = (Correlator) obj;
        if (condition == null) {
            if (other.condition != null)
                return false;
        } else if (!condition.equals(other.condition))
            return false;
        if (left == null) {
            if (other.left != null)
                return false;
        } else if (!left.equals(other.left))
            return false;
        if (right == null) {
            if (other.right != null)
                return false;
        } else if (!right.equals(other.right))
            return false;
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public EventSchema computeOutputSchema(EventSchema... inputSchemas) throws SchemaException {
        EventSchema left = inputSchemas[0];
        EventSchema right = inputSchemas[1];
        return left.union(right, leftAlias, rightAlias);
    }
}
