package sigmod2021.db.core.primaryindex;

import sigmod2021.db.core.primaryindex.queries.range.AttributeRange;
import sigmod2021.db.util.TimeInterval;
import xxl.core.indexStructures.AggregateIndex.Aggregation;

import java.util.List;

public interface HistogramAccess {
    long getIndex();

    Aggregation getAggregates(String attribute);

    TimeInterval getCoveredTimeInterval();

    long getEventCount();

    boolean intersects(List<? extends AttributeRange<? extends Number>> ranges);

    boolean intersects(AttributeRange<? extends Number> range);
}
