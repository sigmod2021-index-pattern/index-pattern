package sigmod2021.esp.bridges.nat.epa.impl.pattern.automaton;

import dk.brics.automaton.Automaton;
import dk.brics.automaton.RegExp;
import dk.brics.automaton.State;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.PatternMatcher;
import sigmod2021.esp.api.epa.pattern.symbol.Binding;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.epa.pattern.symbol.Symbol;
import sigmod2021.esp.bindings.BoundVariables;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.Event;
import sigmod2021.event.EventSchema;

import java.util.*;

/**
 * Represents an automaton accepting the runs expressed by a pattern
 */
public class BufferAutomaton {
    private final PatternMatcher pm;

    private final Map<Character, EVSymbol> translatedSymbols;

    private final int numberOfBindings;

    private final OutputMapping outputMapping;

    private final BufferState initialState;


    public BufferAutomaton(PatternMatcher def, EventSchema inputSchema) throws TranslatorException, IncompatibleTypeException {

        this.pm = def;

        this.translatedSymbols = new HashMap<>();

        List<Binding> previousBindings = new ArrayList<>();
        for (Symbol s : pm.getSymbols()) {
            Bindings tmp = new Bindings(previousBindings.toArray(new Binding[previousBindings.size()]));
            EVSymbol evs = EVSymbol.create(s, inputSchema, tmp);

            translatedSymbols.put(evs.getId(), evs);

            for (Binding b : s.getBindings())
                previousBindings.add(b);

        }

        this.numberOfBindings = previousBindings.size();
        this.outputMapping = new OutputMapping(pm.getSymbols(), pm.getOutput());


        // Create states
        {
            Automaton automaton = new RegExp(def.getPattern().toString()).toAutomaton();
            Map<State, BufferState> stateMap = new HashMap<>();
            State initState = automaton.getInitialState();
            stateMap.put(initState, new BufferState(true, false));

            for (State s : automaton.getStates()) {
                // Add state to map, if not already contained
                if (!stateMap.containsKey(s))
                    stateMap.put(s, new BufferState(false, s.isAccept()));
                Set<dk.brics.automaton.Transition> t = s.getTransitions();
                for (dk.brics.automaton.Transition transition : t) {
                    // Add destination state to map, if not already contained
                    if (!stateMap.containsKey(transition.getDest())) {
                        stateMap.put(transition.getDest(), new BufferState(false, transition.getDest().isAccept()));
                    }
                    Transition.Type type = (s == transition.getDest()) ? Transition.Type.TAKE : Transition.Type.PROCEED;
                    stateMap.get(s).addTransition(
                            new Transition(translatedSymbols.get(transition.getMax()),
                                    stateMap.get(transition.getDest()), type));
                }
            }
            this.initialState = stateMap.get(initState);
        }
    }

    public boolean startsRun(Event e) {
        for (Transition t : initialState.transitions) {
            if (t.symbol.getCondition().eval(e))
                return true;
        }
        return false;
    }

    public Run newRun() {
        return new Run();
    }


    /**
     * Represents a state in the automaton
     */
    static class BufferState {

        /**
         * Contains all transitions from this state
         */
        List<Transition> transitions;

        /**
         * Indicates if this state is accepting
         */
        private boolean accept;

        /**
         * Creates a new buffer state.
         *
         * @param initial flag indicating whether this state is an initial state
         * @param accept  flag indicating whether this state is an accepting state
         */
        public BufferState(boolean initial, boolean accept) {
            transitions = new ArrayList<>();
            this.accept = accept;
        }

        /**
         * Returns if this state is an accepting state.
         *
         * @return <tt>true</tt> if this state is accepting, <tt>false</tt> otherwise
         */
        public boolean isAccept() {
            return accept;
        }

        /**
         * Adds an outgoing transition to this state.
         *
         * @param transition the new transition
         */
        protected void addTransition(Transition transition) {
            transitions.add(transition);
            Collections.sort(transitions);
        }
    }


    /**
     * Represents a transition in the automaton.
     */
    static class Transition implements Comparable<Transition> {

        final EVSymbol symbol;

        ;
        final BufferState target;
        final Type type;
        Transition(EVSymbol symbol, BufferState target, Type type) {
            this.symbol = symbol;
            this.target = target;
            this.type = type;
        }

        /**
         * @{inheritDoc}
         */
        @Override
        public int compareTo(Transition o) {
            return type.compareTo(o.type);
        }

        static enum Type {PROCEED, TAKE}
    }


    /**
     * Represents a run of the pattern matcher.
     */
    public class Run {

        private List<Event> events;

        private BoundVariables variables;

        private BufferState currentState;

        /**
         * Creates a new Run instance
         */
        public Run() {
            events = new ArrayList<>();
            variables = new BoundVariables(numberOfBindings);
            currentState = initialState;
        }

//        private Run(Run copy) {
//            events = new ArrayList<>(copy.events);
//            variables = copy.variables.copy();
//            currentState = copy.currentState;
//        }

        public boolean update(Event e) {
//            List<Run> result = new ArrayList<>();
            for (Transition t : currentState.transitions) {
                if (t.symbol.getCondition().eval(events, e, variables)) {
                    t.symbol.updateBindings(events, e, variables);
                    currentState = t.target;
                    events.add(e);
                    return true;
//                    Run clone = new Run(this);
                    //TODO: Dirty Hack
//                    t.symbol.updateBindings(events, e, clone.variables);
//                    clone.currentState = t.target;
//                    clone.events.add(e);
//                    result.add(clone);
                    // Break on first valid transition
//                    break;
                }
            }
            return false;
        }

        public boolean isAccept() {
            return currentState.isAccept();
        }

        public boolean isTimedOut(long ts) {
            return !events.isEmpty() && ts - events.get(0).getT1() >= pm.getWithin();
        }

        public Event createResult() {
            return outputMapping.create(variables, events.get(events.size() - 1).getT1());
        }
    }

}
