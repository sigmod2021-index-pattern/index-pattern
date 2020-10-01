package xxl.core.cursor;

import java.util.Iterator;


/**
 *
 */
public class LimitingIterator<T> implements Iterator<T> {
	
	private final long limit;
	
	private final Iterator<T> input;
	
	private long count;
	
	/**
	 * Creates a new LimitingIterator instance
	 * @param limit
	 * @param input
	 */
	public LimitingIterator(long limit, Iterator<T> input) {
		this.limit = limit;
		this.input = input;
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public boolean hasNext() {
		return input.hasNext() && count < limit;
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public T next() {
		T res = input.next();
		count++;
		return res;
	}

}
