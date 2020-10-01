package sigmod2021.esp.bridges.nat.epa.impl.pattern.automaton;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.bridge.EPACallback;
import sigmod2021.esp.bridges.nat.epa.impl.pattern.automaton.BufferAutomaton.Run;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.Event;

import java.util.*;

public class AutomatonPatternMatcher {

    private final BufferAutomaton automaton;

    /**
     * The qualifying runs
     */
    Queue<Run> resultRuns = new ArrayDeque<>();

    /**
     * The currently active runs
     */
    List<Run> runs = new LinkedList<>();

    /**
     * The callback for results
     */
    private EPACallback callback;

    // =================================================================================================================

    /**
     * Creates a new pattern matcher for the given pattern definition.
     *
     * @param automaton the pattern definition
     * @throws IncompatibleTypeException
     * @throws TranslatorException
     */
    public AutomatonPatternMatcher(BufferAutomaton automaton) {
        this.automaton = automaton;
    }

    /**
     * The actual matching process
     *
     * @param event
     */
    public void match(Event event) {
        if (automaton.startsRun(event)) {
            runs.add(automaton.newRun());
        }

        for (Iterator<Run> ri = runs.iterator(); ri.hasNext(); ) {
            Run r = ri.next();

            if (r.isTimedOut(event.getT1()) || !r.update(event)) {
                ri.remove();
            } else if (r.isAccept()) {
                resultRuns.add(r);
                ri.remove();
            }
        }
        while (!resultRuns.isEmpty()) {
            callback.receive(resultRuns.poll().createResult());
        }
    }

    public void setCallback(EPACallback callback) {
        this.callback = callback;
    }
}
