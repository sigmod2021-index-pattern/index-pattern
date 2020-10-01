package sigmod2021.esp.expressions.predicate.string;

import sigmod2021.esp.expressions.arithmetic.EVStringExpression;
import sigmod2021.esp.expressions.predicate.EVPredicate;
import sigmod2021.esp.expressions.util.EVAbstractBinaryExpression;

/**
 * Compares two String inputs for Like Matching.
 */
public class EVStringLike extends EVAbstractBinaryExpression<String, String, Boolean> implements EVPredicate {

    public EVStringLike(EVStringExpression left, EVStringExpression right) {
        super(left, right, new OperatorImpl<String, String, Boolean>() {

            @Override
            public Boolean apply(String l, String regex) {
                regex = regex
                        .replace("\\", "\\\\") //escape special java chars 
                        .replace(".", "\\.").replace("^", "\\^")
                        .replace("$", "\\$").replace("|", "\\|")
                        .replace("{", "\\{").replace("}", "\\}")
                        .replaceAll("(?<!!)%", ".*") //replace % with .* (but not !%)
                        .replaceAll("(?<!!)_", ".") //replace _ with . (but not !_)
                        .replace("!%", "%")
                        .replace("!_", "_");
                return l.toLowerCase().matches(regex);
            }
        });
    }

}
