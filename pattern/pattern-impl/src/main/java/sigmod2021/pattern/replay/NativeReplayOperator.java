package sigmod2021.pattern.replay;

import sigmod2021.db.core.primaryindex.PrimaryIndex;
import sigmod2021.pattern.util.NativeOperatorWrapper;
import sigmod2021.esp.api.bridge.EventChannel;
import sigmod2021.esp.api.epa.EPA;
import sigmod2021.esp.bridges.nat.epa.NativeOperator;

/**
 *
 */
public abstract class NativeReplayOperator<T extends EPA> extends ReplayOperator<NativeOperatorWrapper> {

    private final T def;
    private final NativeOperatorFactory<T> factory;

    /**
     * Creates a new NativeReplayOperator instance
     */
    protected NativeReplayOperator(PrimaryIndex tree, T def, NativeOperatorFactory<T> f) {
        super(tree);
        this.def = def;
        this.factory = f;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    protected NativeOperatorWrapper createEPA() {
        return new NativeOperatorWrapper(new NativeOperatorWrapper.OperatorFactory() {

            @Override
            public EPA getDefinition() {
                return def;
            }

            @Override
            public NativeOperator create(EventChannel... inputChannels) {
                return factory.create(def, inputChannels);
            }
        });
    }


    public static interface NativeOperatorFactory<T extends EPA> {
        NativeOperator create(T def, EventChannel... inputChannels);
    }
}
