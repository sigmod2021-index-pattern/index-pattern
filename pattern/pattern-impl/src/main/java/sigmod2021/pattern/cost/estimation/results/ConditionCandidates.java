package sigmod2021.pattern.cost.estimation.results;

import sigmod2021.pattern.cost.estimation.SubTreeDescription;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Holds a set of candidate intervals for a sub-pattern.
 *
 */
public class ConditionCandidates implements Iterable<ConditionCandidate> {

    final List<ConditionCandidate> matches = new ArrayList<>();

    void update(SubTreeDescription std, int conditionIndex) {
        if (!std.getInfos().get(conditionIndex).isHit())
            return;

        ConditionCandidate m = matches.isEmpty() ? null : matches.get(matches.size() - 1);

        if (m == null || std.getId() - m.lastId != 1) {
            m = new ConditionCandidate(std, conditionIndex);
            matches.add(m);
        } else {
            m.update(std, conditionIndex);
        }
    }

    /**
     * @return the matches
     */
    public List<ConditionCandidate> getMatches() {
        return this.matches;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Iterator<ConditionCandidate> iterator() {
        return matches.iterator();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public String toString() {
        return String.format("ConditionCandidates: %s", this.matches);
    }
}
