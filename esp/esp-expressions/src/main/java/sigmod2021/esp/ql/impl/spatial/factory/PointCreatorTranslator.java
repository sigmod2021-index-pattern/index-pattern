package sigmod2021.esp.ql.impl.spatial.factory;

import org.kohsuke.MetaInfServices;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.epa.pattern.symbol.NoSuchVariableException;
import sigmod2021.esp.api.expression.spatial.factory.PointCreator;
import sigmod2021.esp.expressions.spatial.factory.EVPointCreator;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.Translator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

@MetaInfServices
public class PointCreatorTranslator extends Translator<PointCreator, EVPointCreator> {

    public PointCreatorTranslator() {
        super(PointCreator.class, EVPointCreator.class);
    }

    @Override
    protected EVPointCreator process(PointCreator in, ExpressionTranslator translator, EventSchema schema, Bindings bindings) throws TranslatorException, IncompatibleTypeException, NoSuchVariableException, SchemaException {
        return new EVPointCreator(
                translator.translateNumericExpression(in.getInput(0), schema, bindings),
                translator.translateNumericExpression(in.getInput(1), schema, bindings));
    }

}
