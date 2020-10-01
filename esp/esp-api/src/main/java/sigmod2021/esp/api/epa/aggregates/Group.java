package sigmod2021.esp.api.epa.aggregates;

/**
 * Performs grouping on the given attribute.
 * Groupings can be combined across several attributes.
 */
public class Group extends Aggregate {

    private static final long serialVersionUID = 1L;

    /**
     * @param attribute the name of the attribute to group by
     */
    public Group(String attribute) {
        super(attribute, attribute);
    }
}
