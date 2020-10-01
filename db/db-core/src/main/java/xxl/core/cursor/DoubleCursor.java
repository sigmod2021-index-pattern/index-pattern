package xxl.core.cursor;

import java.io.Closeable;
import java.util.NoSuchElementException;

import xxl.core.cursors.Cursor;

/**
 * A BothWaysCursor extends the Cursor interface with additional functionality
 * to go in both directions (forward and backward).
 *
 */
public interface DoubleCursor<E> extends Cursor<E>, Closeable, AutoCloseable {

	/**
	 * Returns if the given cursor supports peeking back one element.
	 *
	 * @return <code>true</code> if the cursor supports peeking back,
	 *         <code>false</code> otherwise
	 */
	boolean supportsPeekBack();

	/**
	 * Returns the previous element without changing the cursor.
	 *
	 * @return the previous element
	 */
	E peekBack() throws IllegalStateException, NoSuchElementException, UnsupportedOperationException;

	/**
	 * Processes the cursor backwards and returns the previous element.
	 *
	 * @return the previous element
	 */
	E previous() throws IllegalStateException, NoSuchElementException;

	/**
	 * Returns if the cursor has a previous element.
	 *
	 * @return <code>true</code> if the cursor has a previous element,
	 *         <code>false</code> otherwise
	 */
	boolean hasPrevious() throws IllegalStateException;
}
