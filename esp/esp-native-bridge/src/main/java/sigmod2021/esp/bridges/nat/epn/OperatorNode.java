package sigmod2021.esp.bridges.nat.epn;

import sigmod2021.esp.api.bridge.EventChannel;
import sigmod2021.esp.bridges.nat.epa.NativeOperator;
import sigmod2021.event.Event;

public class OperatorNode extends StreamNode {

    private final NativeOperator operator;

    public OperatorNode(NativeOperator op) {
        super(op.getSchema());
        this.operator = op;
    }

    public void process(EventChannel input, Event event) {
        this.operator.process(input, event);
    }

    public NativeOperator getOperator() {
        return operator;
    }
}
