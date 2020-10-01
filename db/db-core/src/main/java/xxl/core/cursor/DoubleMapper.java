package xxl.core.cursor;

import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * Mapper implementation for double cursors.
 *
 */
public class DoubleMapper<I,E> implements DoubleCursor<E> {

	private final Function<? super I,? extends E> mapFunction;
	private final Function<? super E,? extends I> mapBack;

	private final DoubleCursor<I> input;
	

	/**
	 * Creates a new DoubleMapper instance
	 * @param function
	 * @param iterators
	 */
	public DoubleMapper(Function<? super I, ? extends E> function, DoubleCursor<I> input) {
		this(function,null,input);
	}
	

	/**
	 * Creates a new DoubleMapper instance
	 * @param mapFunction
	 * @param mapBack
	 * @param input
	 */
	public DoubleMapper(Function<? super I, ? extends E> mapFunction, Function<? super E, ? extends I> mapBack,
			DoubleCursor<I> input) {
		this.mapFunction = mapFunction;
		this.mapBack = mapBack;
		this.input = input;
	}




	/**
	 * @{inheritDoc}
	 */
	@Override
	public void open() {
		input.open();
	}


	/**
	 * @{inheritDoc}
	 */
	@Override
	public void close() {
		input.close();
	}


	/**
	 * @{inheritDoc}
	 */
	@Override
	public boolean hasNext() {
		return input.hasNext();
	}


	/**
	 * @{inheritDoc}
	 */
	@Override
	public E next() {
		return mapFunction.apply(input.next());
	}


	/**
	 * @{inheritDoc}
	 */
	@Override
	public E peek() throws IllegalStateException, NoSuchElementException, UnsupportedOperationException {
		return mapFunction.apply(input.peek());
	}


	/**
	 * @{inheritDoc}
	 */
	@Override
	public void remove() throws IllegalStateException, UnsupportedOperationException {
		input.remove();
	}


	/**
	 * @{inheritDoc}
	 */
	@Override
	public void update(E object) throws IllegalStateException, UnsupportedOperationException {
		if ( !supportsUpdate() )
			throw new UnsupportedOperationException("Update not supported.");

		input.update( mapBack.apply(object) );

	}


	/**
	 * @{inheritDoc}
	 */
	@Override
	public void reset() throws UnsupportedOperationException {
		input.reset();
	}


	/**
	 * @{inheritDoc}
	 */
	@Override
	public E peekBack() {
		return mapFunction.apply(input.peekBack());
	}


	/**
	 * @{inheritDoc}
	 */
	@Override
	public E previous() {
		return mapFunction.apply(input.previous());
	}


	/**
	 * @{inheritDoc}
	 */
	@Override
	public boolean hasPrevious() {
		return input.hasPrevious();
	}


	/**
	 * @{inheritDoc}
	 */
	@Override
	public boolean supportsPeek() {
		return input.supportsPeek();
	}


	/**
	 * @{inheritDoc}
	 */
	@Override
	public boolean supportsPeekBack() {
		return input.supportsPeekBack();
	}


	/**
	 * @{inheritDoc}
	 */
	@Override
	public boolean supportsRemove() {
		return input.supportsRemove();
	}


	/**
	 * @{inheritDoc}
	 */
	@Override
	public boolean supportsUpdate() {
		return input.supportsUpdate() && mapBack != null;
	}


	/**
	 * @{inheritDoc}
	 */
	@Override
	public boolean supportsReset() {
		return input.supportsReset();
	}
	
}
