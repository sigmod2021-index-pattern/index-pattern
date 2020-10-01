package xxl.core.cursor;

import java.util.NoSuchElementException;

/**
 *
 */
public interface MinimalDoubleCursor<T> extends DoubleCursor<T> {

	
	
	/**
	 * @{inheritDoc}
	 */
	@Override
	default boolean supportsPeekBack() {
		return false;
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	default T peekBack() throws IllegalStateException, NoSuchElementException, UnsupportedOperationException {
		throw new UnsupportedOperationException("Peek back not supported by this cursor (" + getClass().getName() + ").");
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	default T peek() throws IllegalStateException, NoSuchElementException, UnsupportedOperationException {
		throw new UnsupportedOperationException("Peek not supported by this cursor (" + getClass().getName() + ").");
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	default boolean supportsPeek() {
		return false;
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	default void remove() throws IllegalStateException, UnsupportedOperationException {
		throw new UnsupportedOperationException("Remove not supported by this cursor (" + getClass().getName() + ").");
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	default boolean supportsRemove() {
		return false;
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	default void update(T object) throws IllegalStateException, UnsupportedOperationException {
		throw new UnsupportedOperationException("Update not supported by this cursor (" + getClass().getName() + ").");
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	default boolean supportsUpdate() {
		return false;
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	default void reset() throws UnsupportedOperationException {
		throw new UnsupportedOperationException("Reset not supported by this cursor (" + getClass().getName() + ").");
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	default boolean supportsReset() {
		return false;
	}
}
