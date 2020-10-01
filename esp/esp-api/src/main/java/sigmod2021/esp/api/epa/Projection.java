package sigmod2021.esp.api.epa;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.event.Attribute;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class Projection implements EPA {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private final AttributeProjection[] projections;
    /** The input-EPA of this filter */
    private EPA input;

    /**
     * @param input
     * @param projections
     */
    public Projection(EPA input, AttributeProjection... projections) {
        this.input = input;
        this.projections = projections;
    }

    public AttributeProjection[] getProjections() {
        return projections;
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
     * {@inheritDoc}
     */
    @Override
    public EventSchema computeOutputSchema(EventSchema... inputSchemas) throws SchemaException {
        List<Attribute> result = new ArrayList<>();

        for (AttributeProjection ap : projections) {
            try {
                Attribute out = new Attribute(ap.getAttributeOut(), ap.expression.getDataType(inputSchemas[0], new Bindings()));
                out.setQualifier(ap.qualifierOut);
                result.add(out);
            } catch (IncompatibleTypeException e) {
                throw new SchemaException("Illegal projection", e);
            }
        }
        return new EventSchema(result.toArray(new Attribute[result.size()]));
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((input == null) ? 0 : input.hashCode());
        result = prime * result + Arrays.hashCode(projections);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Projection other = (Projection) obj;
        if (input == null) {
            if (other.input != null)
                return false;
        } else if (!input.equals(other.input))
            return false;
        if (!Arrays.equals(projections, other.projections))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "Projection [input=" + input + ", projections=" + Arrays.toString(projections) + "]";
    }

    public static class AttributeProjection {
        private final String attributeOut;
        private final String qualifierOut;
        private final ArithmeticExpression expression;

        /**
         * @param attributeOut
         * @param expression
         */
        public AttributeProjection(String attributeOut, ArithmeticExpression expression) {
            this(attributeOut, null, expression);
        }

        /**
         * @param attributeOut
         * @param expression
         */
        public AttributeProjection(String attributeOut, String qualifierOut, ArithmeticExpression expression) {
            this.attributeOut = attributeOut;
            this.qualifierOut = qualifierOut;
            this.expression = expression;
        }

        public String getAttributeOut() {
            return attributeOut;
        }

        public ArithmeticExpression getExpression() {
            return expression;
        }

        /**
         * @{inheritDoc}
         */
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((this.attributeOut == null) ? 0 : this.attributeOut.hashCode());
            result = prime * result + ((this.expression == null) ? 0 : this.expression.hashCode());
            result = prime * result + ((this.qualifierOut == null) ? 0 : this.qualifierOut.hashCode());
            return result;
        }

        /**
         * @{inheritDoc}
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            AttributeProjection other = (AttributeProjection) obj;
            if (this.attributeOut == null) {
                if (other.attributeOut != null)
                    return false;
            } else if (!this.attributeOut.equals(other.attributeOut))
                return false;
            if (this.expression == null) {
                if (other.expression != null)
                    return false;
            } else if (!this.expression.equals(other.expression))
                return false;
            if (this.qualifierOut == null) {
                if (other.qualifierOut != null)
                    return false;
            } else if (!this.qualifierOut.equals(other.qualifierOut))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return qualifierOut == null ? attributeOut + "<-" + expression : qualifierOut + "." + attributeOut + "<-" + expression;
        }
    }


}
