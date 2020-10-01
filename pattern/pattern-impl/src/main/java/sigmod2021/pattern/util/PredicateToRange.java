package sigmod2021.pattern.util;

import sigmod2021.db.core.primaryindex.queries.range.*;
import sigmod2021.esp.api.expression.BooleanExpression;
import sigmod2021.esp.api.expression.Predicate;
import sigmod2021.esp.api.expression.arithmetic.atomic.Constant;
import sigmod2021.esp.api.expression.arithmetic.atomic.Variable;
import sigmod2021.esp.api.expression.logical.And;
import sigmod2021.esp.api.expression.logical.False;
import sigmod2021.esp.api.expression.logical.True;
import sigmod2021.esp.api.expression.predicate.*;
import sigmod2021.event.Attribute;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 *
 */
public final class PredicateToRange {

    /**
     * Creates a new PredicateToRange instance
     */
    private PredicateToRange() {
    }

    public static AttributeRange<?>[] translateArr(EventSchema schema, BooleanExpression expr) throws SchemaException {
        var res = translate(schema, expr);
        return res.ranges.toArray(new AttributeRange<?>[res.ranges.size()]);
    }

    public static TranslationResult translate(EventSchema schema, BooleanExpression expr) throws SchemaException {
        boolean exact = true;
        List<AttributeRange<?>> result = new ArrayList<>();
        if (expr instanceof And) {
            And and = (And) expr;
            for (int i = 0; i < and.getArity(); i++) {
                var tr = translate(schema, and.getInput(i));
                result.addAll(tr.ranges);
                exact &= tr.exact;
            }
        } else if (expr instanceof Predicate) {
            Optional<AttributeRange<?>> translation = translatePredicate(schema, (Predicate) expr);
            if (translation.isPresent()) result.add(translation.get());
            else exact = false;
        } else if (expr instanceof True | expr instanceof False) {
        } else
            throw new IllegalArgumentException("Expressions of type: " + expr + " not yet supported.");
        return new TranslationResult(result, exact);
    }

    private static Optional<AttributeRange<?>> translatePredicate(EventSchema schema, Predicate p) throws SchemaException {
        try {
            if (p instanceof GreaterEq) {
                GreaterEq geq = (GreaterEq) p;
                Variable v = (Variable) geq.getInput(0);
                Constant<?> c = (Constant<?>) geq.getInput(1);
                return Optional.of(toRange(schema.byName(v.getName()), c.getValue(), null, true, true));
            } else if (p instanceof Greater) {
                Greater geq = (Greater) p;
                Variable v = (Variable) geq.getInput(0);
                Constant<?> c = (Constant<?>) geq.getInput(1);
                return Optional.of(toRange(schema.byName(v.getName()), c.getValue(), null, false, true));
            } else if (p instanceof LessEq) {
                LessEq leq = (LessEq) p;
                Variable v = (Variable) leq.getInput(0);
                Constant<?> c = (Constant<?>) leq.getInput(1);
                return Optional.of(toRange(schema.byName(v.getName()), null, c.getValue(), true, true));
            } else if (p instanceof Less) {
                Less leq = (Less) p;
                Variable v = (Variable) leq.getInput(0);
                Constant<?> c = (Constant<?>) leq.getInput(1);
                return Optional.of(toRange(schema.byName(v.getName()), null, c.getValue(), true, false));
            } else if (p instanceof Between) {
                Between b = (Between) p;
                Variable v = (Variable) b.getInput(0);
                Constant<?> l = (Constant<?>) b.getInput(1);
                Constant<?> u = (Constant<?>) b.getInput(2);
                return Optional.of(toRange(schema.byName(v.getName()), l.getValue(), u.getValue(), true, true));
            } else if (p instanceof Equal) {
                Equal eq = (Equal) p;
                Variable v = (Variable) eq.getInput(0);
                Constant<?> c = (Constant<?>) eq.getInput(1);
                return Optional.of(toRange(schema.byName(v.getName()), c.getValue(), c.getValue(), true, true));
            }
        } catch (Exception e) {
        }
        return Optional.empty();
    }

    private static AttributeRange<?> toRange(Attribute a, Object lower, Object upper, boolean li, boolean ui) {
        switch (a.getType()) {
            case BYTE:
                return new ByteAttributeRange(
                        a.getName(),
                        lower != null ? (Byte) lower : Byte.MIN_VALUE,
                        upper != null ? (Byte) upper : Byte.MAX_VALUE, li, ui);
            case SHORT:
                return new ShortAttributeRange(
                        a.getName(),
                        lower != null ? (Short) lower : Short.MIN_VALUE,
                        upper != null ? (Short) upper : Short.MAX_VALUE, li, ui);
            case INTEGER:
                return new IntAttributeRange(
                        a.getName(),
                        lower != null ? (Integer) lower : Integer.MIN_VALUE,
                        upper != null ? (Integer) upper : Integer.MAX_VALUE, li, ui);
            case LONG:
                return new LongAttributeRange(
                        a.getName(),
                        lower != null ? (Long) lower : Long.MIN_VALUE,
                        upper != null ? (Long) upper : Long.MAX_VALUE, li, ui);
            case FLOAT:
                return new FloatAttributeRange(
                        a.getName(),
                        lower != null ? (Float) lower : Float.NEGATIVE_INFINITY,
                        upper != null ? (Float) upper : Float.POSITIVE_INFINITY, li, ui);
            case DOUBLE:
                return new DoubleAttributeRange(
                        a.getName(),
                        lower != null ? (Double) lower : Double.NEGATIVE_INFINITY,
                        upper != null ? (Double) upper : Double.POSITIVE_INFINITY, li, ui);
            case STRING:
                return new StringAttributeRange(
                        a.getName(),
                        (String) lower,
                        (String) upper, li, ui);
            default:
                throw new IllegalArgumentException("Range for attribute " + a + " not supported.");
        }
    }

    public static class TranslationResult {
        private final List<AttributeRange<?>> ranges;
        private final boolean exact;

        /**
         * Creates a new TranslationResult instance
         * @param ranges
         * @param exact
         */
        public TranslationResult(List<AttributeRange<?>> ranges, boolean exact) {
            this.ranges = ranges;
            this.exact = exact;
        }

        /**
         * @return the ranges
         */
        public List<AttributeRange<?>> getRanges() {
            return this.ranges;
        }

        /**
         * @return the exact
         */
        public boolean isExact() {
            return this.exact;
        }
    }

}
