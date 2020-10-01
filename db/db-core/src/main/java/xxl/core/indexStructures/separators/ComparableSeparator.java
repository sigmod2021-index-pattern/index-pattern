package xxl.core.indexStructures.separators;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.indexStructures.Separator;

/**
 * Separator for comparable types.
 *
 */
public class ComparableSeparator extends Separator {


	public static final Function FACTORY_FUNCTION = new AbstractFunction() {
		public Object invoke(Object value){
			return new ComparableSeparator((Comparable)value);
		}
	};

	public ComparableSeparator(Comparable value) {
		super(value);
	}

	@Override
	public Object clone() {
        Comparable result;
        if (this.sepValue instanceof Byte)
            result = new Byte(((Byte)this.sepValue).byteValue());
        else if (this.sepValue instanceof Short)
            result = new Short(((Short)this.sepValue).shortValue());
        else if (this.sepValue instanceof Integer)
            result = new Integer(((Integer)this.sepValue).intValue());
        else if (this.sepValue instanceof Long)
            result = new Long(((Long)this.sepValue).longValue());
        else if (this.sepValue instanceof Float)
            result = new Float(((Float)this.sepValue).floatValue());
        else if (this.sepValue instanceof Double)
            result = new Double(((Double)this.sepValue).doubleValue());
        else if (this.sepValue instanceof String)
            result = new String(((String)this.sepValue));
        else
            throw new RuntimeException("Type not supported: "+this.sepValue.getClass());
		return new ComparableSeparator(result);
	}
}
