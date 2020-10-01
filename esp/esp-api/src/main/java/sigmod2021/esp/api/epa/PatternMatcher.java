package sigmod2021.esp.api.epa;

import sigmod2021.common.EPException;
import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.Output;
import sigmod2021.esp.api.epa.pattern.Partition;
import sigmod2021.esp.api.epa.pattern.regex.Pattern;
import sigmod2021.esp.api.epa.pattern.symbol.Binding;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.epa.pattern.symbol.Symbol;
import sigmod2021.esp.api.epa.pattern.symbol.Symbols;
import sigmod2021.event.Attribute;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

import java.util.ArrayList;
import java.util.List;

/**
 * Representation of a Patterm-Matcher-Agent.
 * Searches for a pattern in a sequence of events.
 * The pattern is represented by a regular expression over {@link Symbol symbols}.
 * Each symbol holds a condition which must be satisfied by incoming events in order
 * to yield this symbol.
 * A result is reported, if the sequence of symbols described by the regular expression is found.
 */
public class PatternMatcher implements EPA {

    private static final long serialVersionUID = 1L;
    /**
     * The time-window in which the pattern must occur completely
     */
    private final long within;
    /**
     * The symbol definitions
     */
    private final Symbols symbols;
    /**
     * The pattern definition
     */
    private final Pattern pattern;
    /**
     * The output definition
     */
    private final Output output;
    /**
     * The attributes of the input-stream used for partitioning -- optional
     */
    private final Partition partitionBy;
    /**
     * The input stream
     */
    private EPA input;

    /**
     * @param input   the input stream
     * @param within  The time-window in which the pattern must occur completely
     * @param symbols The symbol definitions
     * @param pattern The pattern definition
     * @param output  The output definition
     */
    public PatternMatcher(EPA input, long within, Symbols symbols, Pattern pattern, Output output) {
        this(input, within, symbols, pattern, output, new Partition());
    }

    /**
     * @param input       the input stream
     * @param within      The time-window in which the pattern must occur completely
     * @param symbols     The symbol definitions
     * @param pattern     The pattern definition
     * @param output      The output definition
     * @param partitionBy The attributes of the input-stream used for partitioning
     */
    public PatternMatcher(EPA input, long within, Symbols symbols, Pattern pattern, Output output, Partition partitionBy) {
        super();
        this.input = input;
        this.within = within;
        this.symbols = symbols;
        this.pattern = pattern;
        this.output = output;
        this.partitionBy = partitionBy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public EPA[] getInputEPAs() {
        return new EPA[]{input};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setInput(int idx, EPA input) {
        if (idx == 0)
            this.input = input;
        else
            throw new IllegalArgumentException("Invalid input index: " + idx);
    }

    /**
     * @return The time-window in which the pattern must occur completely
     */
    public long getWithin() {
        return within;
    }

    /**
     * @return The pattern definition
     */
    public Pattern getPattern() {
        return pattern;
    }

    /**
     * @return The symbol definitions
     */
    public Symbols getSymbols() {
        return symbols;
    }

    /**
     * @return The output definition
     */
    public Output getOutput() {
        return output;
    }

    /**
     * @return The attributes of the input-stream used for partitioning
     */
    public Partition getPartitionBy() {
        return partitionBy;
    }

    /**
     * {@inheritDoc}
     *
     * @throws IncompatibleTypeException
     */
    @Override
    public EventSchema computeOutputSchema(EventSchema... inputSchemas) throws SchemaException {
        try {
            List<Attribute> attributes = new ArrayList<>();
            Bindings allBindings = symbols.getAllBindings();
            for (String var : output) {
                Binding b = symbols.findBinding(var);
                attributes.add(new Attribute(var, b.getValue().getDataType(inputSchemas[0], allBindings)));
            }
            return new EventSchema(attributes.toArray(new Attribute[attributes.size()]));
        } catch (EPException epex) {
            throw new SchemaException("Could not compute output schema", epex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "PatternMatcher [input=" + input + ", within=" + within + ", symbols=" + symbols + ", pattern=" + pattern + ", output=" + output
                + ", partitionBy=" + partitionBy + "]";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((input == null) ? 0 : input.hashCode());
        result = prime * result + ((output == null) ? 0 : output.hashCode());
        result = prime * result + ((partitionBy == null) ? 0 : partitionBy.hashCode());
        result = prime * result + ((pattern == null) ? 0 : pattern.hashCode());
        result = prime * result + ((symbols == null) ? 0 : symbols.hashCode());
        result = prime * result + (int) (within ^ (within >>> 32));
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        PatternMatcher other = (PatternMatcher) obj;
        if (input == null) {
            if (other.input != null)
                return false;
        } else if (!input.equals(other.input))
            return false;
        if (output == null) {
            if (other.output != null)
                return false;
        } else if (!output.equals(other.output))
            return false;
        if (partitionBy == null) {
            if (other.partitionBy != null)
                return false;
        } else if (!partitionBy.equals(other.partitionBy))
            return false;
        if (pattern == null) {
            if (other.pattern != null)
                return false;
        } else if (!pattern.equals(other.pattern))
            return false;
        if (symbols == null) {
            if (other.symbols != null)
                return false;
        } else if (!symbols.equals(other.symbols))
            return false;
        if (within != other.within)
            return false;
        return true;
    }
}
