package sigmod2021.pattern.cost.selection;

import sigmod2021.db.core.secondaryindex.SecondaryTimeIndex;
import sigmod2021.db.core.primaryindex.impl.PrimaryIndexImpl;
import sigmod2021.pattern.cost.transform.TransformedPattern;

import java.util.List;

/**
 *
 */
public interface IndexSelectionStrategyFactory {

    IndexSelectionStrategy create(PrimaryIndexImpl primary, List<? extends SecondaryTimeIndex<?>> secondaries,
                                  TransformedPattern pattern, PatternStats result);

}
