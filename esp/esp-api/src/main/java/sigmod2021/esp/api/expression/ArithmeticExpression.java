package sigmod2021.esp.api.expression;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

/**
 * Interface for any arithmetic expression.
 * Arithmetic expression in this case means, that it is evaluated
 * to a known {@link DataType}.
 */
public interface ArithmeticExpression extends Expression {

    /**
     * {@inheritDoc}
     */
    ArithmeticExpression getInput(int index);

    /**
     * The data type of this expression
     *
     * @param schema   the schema of incoming events
     * @param bindings the active variable bindings
     * @return the type of this expression
     * @throws IncompatibleTypeException if input-types do not match this expression's specification
     * @throws SchemaException           if the referenced attribute is not part of the given schema
     */
    DataType getDataType(EventSchema schema, Bindings bindings) throws IncompatibleTypeException, SchemaException;

}
