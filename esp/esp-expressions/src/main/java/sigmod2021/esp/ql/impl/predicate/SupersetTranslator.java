package sigmod2021.esp.ql.impl.predicate;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.predicate.set.Superset;
import sigmod2021.esp.expressions.predicate.set.EVSuperset;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

/**
 *
 */
@MetaInfServices
public class SupersetTranslator extends Translator<Superset, EVSuperset> {

    public SupersetTranslator() {
        super(Superset.class, EVSuperset.class);
    }

    @Override
    protected EVSuperset process(Superset in, ExpressionTranslator translator, EventSchema schema, Bindings bindings)
            throws TranslatorException, IncompatibleTypeException, SchemaException {
        ArithmeticExpression l = in.getInput(0);
        ArithmeticExpression r = in.getInput(1);

        DataType lt = l.getDataType(schema, bindings);
        DataType rt = r.getDataType(schema, bindings);

        if (lt == DataType.SET && rt == DataType.SET) {
            return new EVSuperset(translator.translateSetExpression(l, schema, bindings), translator.translateSetExpression(r, schema, bindings));
        } else
            throw new IncompatibleTypeException("No implementation for Subset with input-types: " + lt + " and " + rt);
    }
}
