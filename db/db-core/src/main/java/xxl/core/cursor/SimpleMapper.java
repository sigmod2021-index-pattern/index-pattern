package xxl.core.cursor;

import java.util.function.Function;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;

/**
 * Mapper implementation for double cursors.
 *
 */
public class SimpleMapper<I,E> extends AbstractCursor<E> {

	private final Function<? super I,? extends E> mapFunction;

	private final Cursor<? extends I> input;
	

	/**
	 * Creates a new DoubleMapper instance
	 * @param function
	 * @param iterators
	 */
	public SimpleMapper(Function<? super I, ? extends E> function, Cursor<? extends I> input) {
		this.mapFunction = function;
		this.input = input;
	}


	/**
	 * @{inheritDoc}
	 */
	@Override
	public void open() {
		if ( isOpened )
			return;

		super.open();
		input.open();
	}


	/**
	 * @{inheritDoc}
	 */
	@Override
	public void close() {
		if ( isClosed )
			return;
		super.close();
		input.close();
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	protected boolean hasNextObject() {
		return input.hasNext();
	}


	/**
	 * @{inheritDoc}
	 */
	@Override
	protected E nextObject() {
		return mapFunction.apply(input.next());
	}


	
}
