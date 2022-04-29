package sigmod2021.pattern.cost.estimation.results;

import sigmod2021.pattern.cost.estimation.SubTreeDescription;
import sigmod2021.esp.api.util.RingBuffer;

import java.util.*;
import java.util.stream.Collectors;

public class WindowedPatternRunner {

    /** The current window content */
    private final RingBuffer<SubTreeDescription> windowContent = new RingBuffer<>();

    /** The currently valid sub-pattern hits */
    private final List<Deque<SubTreeDescription>> valids = new ArrayList<>();

    /** The result intervals */
    private final List<SubTreeDescription> result = new ArrayList<>();

    /** The time window */
    private final long window;


    public WindowedPatternRunner(int conditionCount, long window) {
        this.window = window;
        for (int i = 0; i < conditionCount; i++)
            valids.add(new ArrayDeque<>());
    }

    // This updates the window with candidate information for a sub-tree
    public void update(SubTreeDescription std) {
        // If this is the first subtree
        if (windowContent.isEmpty()) {
            add(std);
        }
        // If window is not full
        else if (windowContent.peekFirst().getInterval().getT2() + window > std.getInterval().getT1()) {
            add(std);
        }
        // Only add stuff, if a match may start
        else {
            addSubtrees();
            removeFirst();
            add(std);
        }
    }

    // Adds new information to the window
    private void add(SubTreeDescription std) {
        // Push subtree info to window
        windowContent.add(std);

        // Maintain for each condition the sub-trees that own a candidate for it
        for (int i = 0; i < valids.size(); i++) {
            if (std.getInfos().get(i).isHit())
                valids.get(i).add(std);
        }
    }

    // Removes the expired information (content that leaves the window)
    private void removeFirst() {
        var ref = windowContent.pollFirst();
        // Clean up sub-pattern specific information
        for (var v : valids) {
            if (!v.isEmpty() && v.peekFirst().getId() == ref.getId())
                v.pollFirst();
        }
    }

    // Checks if a match is possible starting with the current window 
    private void addSubtrees() {
        // Check if begin of window holds candidate for first condition
        if (!windowContent.peekFirst().getInfos().get(0).isHit())
            return;


        // If so, remember id 
        // This is required to only add subtrees which were not add by a previous match to the global scan range
        final long firstIndex = windowContent.peekFirst().getId();

        // Check for a sequential match
        // Loop through conditions and pick first candidate with id greater than or equal to the id of candidate for prev. sub-pattern
        // Additionally remove hits which cannot be part of a match anymore
        long currentIdx = firstIndex;

        Deque<SubTreeDescription> conditionStds = new ArrayDeque<>();
        conditionStds.add(windowContent.peekFirst());

        int cIdx = 1;

        while ( !conditionStds.isEmpty() && conditionStds.size() < valids.size() ) {
            var v = valids.get(cIdx);
            // Removal
            while (!v.isEmpty() && v.peekFirst().getId() < currentIdx)
                v.pollFirst();
            // Nothing left
            if (v.isEmpty())
                return;

            long pIdx = v.peekFirst().getId();
            var currentId = v.peekFirst().getInfos().get(cIdx).getCondition().getId();

            // Same sub-pattern, new symbol, one or more subtrees in between -> Rewind sub-pattern
            if ( currentId.absolutePosition > 0 && currentId.conditionIndex == 0 &&
                    (currentIdx - conditionStds.peekLast().getId()) > 1 ) {


                // If is first sub-pattern, matches will be caught on next iteration
                if ( currentId.subPatternIndex == 0 )
                    return;

                for (cIdx = cIdx - 1; cIdx >= 0 &&
                        conditionStds.peekLast().getInfos().get(cIdx).getCondition().getId().isSameSubPattern(currentId); cIdx-- ) {
                    currentIdx = conditionStds.pollLast().getId()+1;
                }
            }
            // Same symbol different subtree
            else if (currentId.conditionIndex > 0 && pIdx > currentIdx) {
                // If is first symbol, matches will be caught on next iteration
                if ( currentId.subPatternIndex == 0 && currentId.absolutePosition == 0 )
                    return;

                for (cIdx = cIdx - 1; cIdx >= 0 &&
                        conditionStds.peekLast().getInfos().get(cIdx).getCondition().getId().isSameSymbol(currentId); cIdx-- ) {
                    currentIdx = conditionStds.pollLast().getId()+1;
                }

            }
            else {
                conditionStds.add(v.peekFirst());
                currentIdx = pIdx;
            }
            cIdx++;
        }

        if ( conditionStds.size() < valids.size() )
            return;

        // Compute window portion to add
        // This is the last candidate for the last sub-pattern
//		final long endIndex = valids.get(valids.size()-1).peekLast().getId();
        final long endIndex = computeEndIndex(firstIndex);

        // The actual start
        long beginIdx = result.isEmpty() ? firstIndex : Math.max(result.get(result.size() - 1).getId() + 1, firstIndex);

        // Calculate offset and add stuff
        long firstWIndex = windowContent.peekFirst().getId();

        for (long i = beginIdx - firstWIndex; i <= endIndex - firstWIndex; i++)
            result.add(windowContent.get((int) i));
    }

    private long computeEndIndex(long firstIdx) {

        int lastSubPatternIndex = -1;

        // Compute first condition of last sub-pattern
        int start = 0;
        for (int i = 0; i < valids.size(); i++) {
            var ci = valids.get(i).peek().getInfos().get(i);
            if (ci.getCondition().getId().subPatternIndex > lastSubPatternIndex) {
                start = i;
                lastSubPatternIndex = ci.getCondition().getId().subPatternIndex;
            }
        }

        // Last sub-pattern is single condition?
        if (start == valids.size() - 1)
            return valids.get(start).peekLast().getId();
            // Only one sub-pattern with one symbol
//		else if ( start == 0 && valids.get(0).peekFirst().getInfos().stream().map(x -> x.getCondition().getMaxDist() ).allMatch( x -> x == 0 ) ) {
        else if (start == 0 && valids.get(0).peekFirst().getInfos().subList(1, valids.get(0).peekFirst().getInfos().size()).stream().map(x -> x.getCondition().getMaxDist()).allMatch(x -> x == 0)) {
            return firstIdx;
        }


        List<Iterator<SubTreeDescription>> iters = valids.subList(start, valids.size()).stream().map(x -> x.descendingIterator()).collect(Collectors.toList());

        SubTreeDescription[] work = new SubTreeDescription[iters.size()];

        Iterator<SubTreeDescription> last = iters.get(iters.size() - 1);

        while (last.hasNext()) {
            work[iters.size() - 1] = last.next();
            if (buildWorkRek(iters.size() - 2, iters, work) && validateWork(work))
                break;

        }
        if (validateWork(work))
            return (work[work.length - 1].getId());
        else
            throw new IllegalStateException("Could not determine last occurrence of last sub-pattern.");
    }

    private boolean buildWorkRek(int step, List<Iterator<SubTreeDescription>> iters, SubTreeDescription[] work) {
        Iterator<SubTreeDescription> iter = iters.get(step);

        while ((work[step] == null || work[step].getId() > work[step + 1].getId()) && iter.hasNext()) {
            work[step] = iter.next();
        }

        if (work[step] == null || work[step].getId() > work[step + 1].getId()) {
            return false;
        } else if (step > 0) {
            return buildWorkRek(step - 1, iters, work);
        } else {
            return true;
        }
    }

    // At most sub-patterns are spread across 2 sub-trees
    private boolean validateWork(SubTreeDescription[] work) {
        return work[0] != null && work[work.length - 1] != null && work[work.length - 1].getId() - work[0].getId() <= 1;
    }

    public List<MatchCandidate> getResults() {
        // Process tail fraction of stream
        while (!windowContent.isEmpty()) {
            addSubtrees();
            removeFirst();
        }

        if (result.isEmpty())
            return Collections.emptyList();

        // Build match candidates (contigous portions of the stream that may contain a match)
        List<MatchCandidate> result = new ArrayList<>();
        Iterator<SubTreeDescription> iter = this.result.iterator();

        MatchCandidate current = new MatchCandidate(iter.next());

        while (iter.hasNext()) {
            var std = iter.next();
            try {
                current.update(std);
                // This is the case, if the item that was tried to add is not a direct successor (i.e., the events are not contigous)
                // Hence, we start a new match candidate
            } catch (IllegalArgumentException iae) {
                // Save candidate
                result.add(current);
                // Start new candidate
                current = new MatchCandidate(std);
            }
        }
        result.add(current);
        return result;
    }
}
