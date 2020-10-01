package xxl.core.indexStructures.separators;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.indexStructures.BPlusTree.KeyRange;

/**
 * Key range for comparable types.
 *
 */
public class ComparableKeyRange extends KeyRange {


	public static final Function FACTORY_FUNCTION = new AbstractFunction() {
		public Object invoke(Object min, Object max){
			return new ComparableKeyRange((Comparable)min, (Comparable)max);
		}
	};

	public ComparableKeyRange(Comparable min, Comparable max) {
		super(min, max);
	}
	
	@Override
	public Object clone() {
        Comparable s;
        Comparable e;
        if (this.minBound() instanceof Byte) {
            s = new Byte(((Byte) this.sepValue).byteValue());
            e = new Byte(((Byte) this.maxBound).byteValue());
        } else if (this.sepValue instanceof Short) {
            s = new Short(((Short) this.sepValue).shortValue());
            e = new Short(((Short) this.maxBound).shortValue());
        } else if (this.sepValue instanceof Integer) {
            s = new Integer(((Integer) this.sepValue).intValue());
            e = new Integer(((Integer) this.maxBound).intValue());
        } else if (this.sepValue instanceof Long) {
            s = new Long(((Long) this.sepValue).longValue());
            e = new Long(((Long) this.maxBound).longValue());
        } else if (this.sepValue instanceof Float) {
            s = new Float(((Float) this.sepValue).floatValue());
            e = new Float(((Float) this.maxBound).floatValue());
        } else if (this.sepValue instanceof Double) {
            s = new Double(((Double) this.sepValue).doubleValue());
            e = new Double(((Double) this.maxBound).doubleValue());
        } else if (this.sepValue instanceof String) {
            s = new String(((String) this.sepValue));
            e = new String(((String) this.maxBound));
        } else
            throw new RuntimeException("Type not supported: "+this.sepValue.getClass());
        return new ComparableKeyRange(s, e);
	}

}
