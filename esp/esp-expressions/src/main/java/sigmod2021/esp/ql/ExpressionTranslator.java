package sigmod2021.esp.ql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Binding;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.epa.pattern.symbol.NoSuchVariableException;
import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.BooleanExpression;
import sigmod2021.esp.api.expression.Expression;
import sigmod2021.esp.bindings.EVBinding;
import sigmod2021.esp.expressions.arithmetic.EVNumericExpression;
import sigmod2021.esp.expressions.arithmetic.EVSetExpression;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.esp.expressions.arithmetic.EVStringExpression;
import sigmod2021.esp.expressions.bool.EVBooleanExpression;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public class ExpressionTranslator {

    public static final ExpressionTranslator INSTANCE = new ExpressionTranslator();
    private static final Logger log = LoggerFactory.getLogger(ExpressionTranslator.class);
    private final TranslatorMapping translators = new TranslatorMapping();


    @SuppressWarnings("rawtypes")
    public ExpressionTranslator() {
        log.debug("Loading registered translators.");

        ServiceLoader<Translator> tsl = ServiceLoader.load(Translator.class);
        for (Translator<?, ?> t : tsl) {
            log.debug("Registering translator: {}", t);
            translators.add(t);
        }
        log.debug("Finished loading registered translators.");
    }

    public EVBinding translateVariableBinding(Binding in, EventSchema schema, Bindings bindings) throws TranslatorException, IncompatibleTypeException {
        try {
            return new EVBinding(in, schema, bindings);
        } catch (NoSuchVariableException e) {
            throw new TranslatorException("Variable not found!", e);
        } catch (SchemaException e) {
            throw new TranslatorException("Error translating binding.", e);
        }
    }

    public EVBooleanExpression translateBooleanExpression(BooleanExpression in, EventSchema schema, Bindings bindings) throws TranslatorException, IncompatibleTypeException {
        return translate(in, EVBooleanExpression.class, schema, bindings);
    }

    public EVStringExpression translateStringExpression(ArithmeticExpression in, EventSchema schema, Bindings bindings) throws TranslatorException, IncompatibleTypeException {
        return translate(in, EVStringExpression.class, schema, bindings);
    }

    public EVNumericExpression translateNumericExpression(ArithmeticExpression in, EventSchema schema, Bindings bindings) throws TranslatorException, IncompatibleTypeException {
        return translate(in, EVNumericExpression.class, schema, bindings);
    }

    public EVSpatialExpression translateSpatialExpression(ArithmeticExpression in, EventSchema schema, Bindings bindings) throws TranslatorException, IncompatibleTypeException {
        return translate(in, EVSpatialExpression.class, schema, bindings);
    }

    public EVSetExpression translateSetExpression(ArithmeticExpression in, EventSchema schema, Bindings bindings) throws TranslatorException, IncompatibleTypeException {
        return translate(in, EVSetExpression.class, schema, bindings);
    }

    private <T> T translate(Expression in, Class<T> desiredType, EventSchema schema, Bindings bindings) throws TranslatorException, IncompatibleTypeException {
        try {
            return desiredType.cast(translators.get(in.getClass(), desiredType).translate(in, this, schema, bindings));
        } catch (NoSuchVariableException e) {
            throw new TranslatorException("Variable not found!", e);
        } catch (SchemaException e) {
            throw new TranslatorException("Error translating expression.", e);
        }
    }

    private static class TranslatorMapping {

        private List<Translator<?, ?>> translators = new ArrayList<>();


        public void add(Translator<?, ?> translator) {
            translators.add(translator);
        }

        @SuppressWarnings("unchecked")
        public <TIN extends Expression, TOUT> Translator<TIN, TOUT> get(Class<TIN> inType, Class<TOUT> outType) throws TranslatorException {
            for (Translator<?, ?> t : translators) {
                if (t.getInputType().isAssignableFrom(inType) && outType.isAssignableFrom(t.getOutputType()))
                    return (Translator<TIN, TOUT>) t;
            }
            throw new TranslatorException("No Translator found: " + inType + " --> " + outType);
        }
    }

}
