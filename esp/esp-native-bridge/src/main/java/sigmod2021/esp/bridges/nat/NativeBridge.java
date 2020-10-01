package sigmod2021.esp.bridges.nat;

import sigmod2021.common.EPException;
import sigmod2021.esp.api.bridge.EPACallback;
import sigmod2021.esp.api.bridge.EPBridge;
import sigmod2021.esp.api.bridge.EventChannel;
import sigmod2021.esp.api.epa.*;
import sigmod2021.esp.api.epa.window.Window;
import sigmod2021.esp.api.provider.NoSuchQueryException;
import sigmod2021.esp.api.provider.NoSuchStreamException;
import sigmod2021.esp.bridges.nat.epa.NativeOperator;
import sigmod2021.esp.bridges.nat.epa.impl.aggregator.NativeAggregator;
import sigmod2021.esp.bridges.nat.epa.impl.correlator.NativeCorrelator;
import sigmod2021.esp.bridges.nat.epa.impl.filter.NativeFilter;
import sigmod2021.esp.bridges.nat.epa.impl.pattern.NativePatternMatcher;
import sigmod2021.esp.bridges.nat.epa.impl.projection.NativeProjection;
import sigmod2021.esp.bridges.nat.epa.impl.window.NativeWindow;
import sigmod2021.esp.bridges.nat.epn.OperatorNode;
import sigmod2021.esp.bridges.nat.epn.StreamNode;
import sigmod2021.event.EventSchema;

import java.util.HashSet;
import java.util.Set;

public class NativeBridge implements EPBridge {

    private Set<StreamNode> streams = new HashSet<>();

    private Set<OperatorNode> operators = new HashSet<>();

    @Override
    public EventChannel registerStream(EventSchema schema) throws EPException {
        StreamNode node = new StreamNode(schema);
        streams.add(node);
        return node;
    }

    @Override
    public EventChannel registerWindow(Window window, EPACallback callback, EventChannel input) throws EPException {
        return registerOperator(NativeWindow.buildWindow(input, window), callback, input);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public EventChannel registerProjection(Projection def, EPACallback callback, EventChannel input) throws EPException {
        return registerOperator(new NativeProjection(def, input), callback, input);
    }

    @Override
    public EventChannel registerFilter(Filter def, EPACallback callback, EventChannel input) throws EPException {
        return registerOperator(new NativeFilter(def, input), callback, input);
    }

    @Override
    public EventChannel registerAggregator(Aggregator def, EPACallback callback, EventChannel input) throws EPException {
        return registerOperator(new NativeAggregator(def, input), callback, input);
    }

    @Override
    public EventChannel registerCorrelator(Correlator def, EPACallback callback, EventChannel input1, EventChannel input2) throws EPException {
        return registerOperator(new NativeCorrelator(def, input1, input2), callback, input1, input2);
    }

    @Override
    public EventChannel registerPatternMatcher(PatternMatcher def, EPACallback callback, EventChannel input) throws EPException {
        return registerOperator(new NativePatternMatcher(def, input), callback, input);
    }

    @Override
    public void removeEPA(EventChannel handle, boolean force, boolean flush) throws NoSuchQueryException {
        if (operators.contains(handle)) {
            OperatorNode n = (OperatorNode) handle;
            if (!force && n.hasSuccessor())
                throw new IllegalStateException("Cannot remove an EPA with registered consumers! Consider using the force flag.");
            else {
                removeAsSuccessor(n);
                operators.remove(n);
                if (flush)
                    n.getOperator().flushState();
            }
        } else
            throw new NoSuchQueryException("No EPA found for the given handle!");
    }

    @Override
    public void removeStream(EventChannel handle, boolean force) throws NoSuchStreamException {
        if (streams.contains(handle)) {
            StreamNode sn = (StreamNode) handle;
            if (!force && sn.hasSuccessor())
                throw new IllegalStateException("Cannot remove an EPA with registered consumers! Consider using the force flag.");
            else
                streams.remove(sn);
        } else
            throw new NoSuchStreamException("No EPA found for the given handle!");
    }

    private OperatorNode registerOperator(NativeOperator operator, EPACallback callback, EventChannel... inputs) {
        operator.setCallback(callback);
        OperatorNode node = new OperatorNode(operator);
        for (EventChannel input : inputs)
            addAsSuccessor(node, input);
        operators.add(node);
        return node;
    }


    private void addAsSuccessor(OperatorNode node, EventChannel input) {
        if (streams.contains(input) || operators.contains(input))
            ((StreamNode) input).addSuccessor(node);
        else
            throw new IllegalArgumentException("ResultStream is unknown!");
    }

    private void removeAsSuccessor(OperatorNode node) {
        for (OperatorNode on : operators)
            on.removeSuccessor(node);
        for (StreamNode sn : streams)
            sn.removeSuccessor(node);
    }

    @Override
    public void destroy() {
        // Nothing to do yet!
    }
}
