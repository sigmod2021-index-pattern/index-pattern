package sigmod2021.esp.bridges.nat.epa.impl.correlator;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.bridge.EPACallback;
import sigmod2021.esp.api.bridge.EventChannel;
import sigmod2021.esp.api.epa.Correlator;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.bridges.nat.epa.NativeOperator;
import sigmod2021.esp.bridges.nat.epa.util.AscTimestampComparator;
import sigmod2021.esp.expressions.bool.EVBooleanExpression;
import sigmod2021.esp.ql.ExpressionTranslator;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.Event;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;
import sigmod2021.event.impl.CompoundEvent;

import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * Implementation of a native binary join over data streams.
 */
public class NativeCorrelator implements NativeOperator {

    private final EventChannel left;

    private final EventChannel right;

    /**
     * Predicate which determines if a pair of given events should be joined.
     * This predicate is user defined and given by the constructor.
     */
    private final EVBooleanExpression joinPredicate;

    private final EventSchema outputSchema;

    private final PriorityQueue<Event> out;
    private final IntervalSweepArea statusLeft;
    private final IntervalSweepArea statusRight;

    // Clocks of the input-streams
    private long minTStartL = Long.MIN_VALUE;
    private long minTStartR = Long.MIN_VALUE;

    private EPACallback callback;

    public NativeCorrelator(Correlator def, EventChannel left, EventChannel right) throws TranslatorException, IncompatibleTypeException, SchemaException {
        this.left = left;
        this.right = right;
        this.outputSchema = def.computeOutputSchema(left.getSchema(), right.getSchema());
        this.joinPredicate = new ExpressionTranslator().translateBooleanExpression(def.getCondition(), this.outputSchema, new Bindings());
        this.out = new PriorityQueue<>(new AscTimestampComparator());
        this.statusLeft = new IntervalSweepArea();
        this.statusRight = new IntervalSweepArea();
    }

    @Override
    public EventSchema getSchema() {
        return outputSchema;
    }

    @Override
    public void setCallback(EPACallback callback) {
        this.callback = callback;
    }

    @Override
    public void process(EventChannel input, Event event) {
        if (input == left)
            pushLeft(event);
        if (input == right)
            pushRight(event);
    }

    /**
     * Called for incoming events on the left side
     *
     * @param event the incoming event
     */
    private void pushLeft(Event event) {
        statusLeft.add(event);

        // Fix this event in the access object
        CompoundEvent access = new CompoundEvent();
        access.setE1(event);

        // Remove all expired events from the right sweep-area
        statusRight.removeWithEndTimeStampLQ(event.getT1());

        // Iterate all potential right partners
        Iterator<Event> iterator = statusRight.queryStartTimestampLess(event.getT2());
        while (iterator.hasNext()) {
            Event potentialPartner = iterator.next();
            access.setE2(potentialPartner);
            if (joinPredicate.eval(access))
                out.add(access.materialize());
        }

        if (event.getT1() >= minTStartL) {
            minTStartL = event.getT1();
            // Flush only if necessary
            // if (tstart <= minTStartR || rightRelation)
            flushInternal();
        }
    }

    /**
     * Called for incoming events on the right side
     *
     * @param event the incoming event
     */
    private void pushRight(Event event) {
        statusRight.add(event);

        // Fix this event in the access object
        CompoundEvent access = new CompoundEvent();
        access.setE2(event);

        // Remove all expired events from the left sweep-area
        statusLeft.removeWithEndTimeStampLQ(event.getT1());

        // Iterate all potential right partners
        Iterator<Event> iterator = statusLeft.queryStartTimestampLess(event.getT2());
        while (iterator.hasNext()) {
            Event potentialPartner = iterator.next();
            access.setE1(potentialPartner);
            if (joinPredicate.eval(access))
                out.add(access.materialize());
        }

        if (event.getT1() >= minTStartR) {
            minTStartR = event.getT1();
            // Flush only if necessary
            // if (tstart <= minTStartL || leftRelation )
            flushInternal();
        }
    }

    /**
     * Flushes the output-heap to the minimum of both input clocks
     */
    protected void flushInternal() {
        long minTS = Math.min(minTStartL, minTStartR);

        while (!out.isEmpty() && out.peek().getT1() <= minTS)
            callback.receive(out.poll());
    }

    public void flushState() {
        while (!out.isEmpty())
            callback.receive(out.poll());
    }
}
