package sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates;

import sigmod2021.esp.api.expression.spatial.temporal.TemporalGeometry;
import sigmod2021.esp.bridges.nat.epa.util.spatial.TemporalGeometryMerger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class TemporalLineStringMergeFunction implements AggregateFunction<List<TemporalGeometry>, TemporalGeometry, TemporalGeometry> {

    /** The serialVersionUID */
    private static final long serialVersionUID = 1L;

    /**
     * @{inheritDoc}
     */
    @Override
    public List<TemporalGeometry> fInit() {
        return Collections.emptyList();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public List<TemporalGeometry> fInit(TemporalGeometry initialValue) {
        return Collections.singletonList(initialValue);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public List<TemporalGeometry> fMerge(List<TemporalGeometry> left, List<TemporalGeometry> right) {
        List<TemporalGeometry> result = new ArrayList<>(left);
        result.addAll(right);
        return result;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public TemporalGeometry fEval(List<TemporalGeometry> value, long counter) {
        @SuppressWarnings("unchecked")
        Iterator<TemporalGeometry.TemporalCoordinate>[] iterators = new Iterator[value.size()];
        for (int i = 0; i < iterators.length; i++) {
            iterators[i] = value.get(i).iterator();
        }
        return new TemporalGeometryMerger(iterators).merge();
    }
}
