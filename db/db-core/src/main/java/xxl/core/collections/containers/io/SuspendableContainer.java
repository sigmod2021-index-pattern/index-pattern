package xxl.core.collections.containers.io;

import java.util.Iterator;

import xxl.core.collections.containers.Container;

/**
 * Interface for containers that can free resources temporarily.
 *
 */
public interface SuspendableContainer extends Container {

	/**
	 * Suspends the container and frees resources temporarily. This method
	 * should be called in order to save system resources. Especially the number
	 * of open file handlers is critical in UNIX-based operating systems.
	 */
	void suspend();

	/**
	 * This method should be called after suspending the container.
	 * Nevertheless, a call to get(), update() of reserve() should also request
	 * the required resources again.
	 */
	void resume();

	/**
	 * Returns if this container has been suspended.
	 *
	 * @return <code>True</code>, if this container has been suspended by
	 *         calling suspend(), <code>false</code> otherwise.
	 */
	boolean isSuspended();

	/**
	 * Returns an iterator that delivers all the identifiers of the container
	 * that are in use in inverted order compared to ids().
	 *
	 * @return the identifiers that are in use in reverse order
	 */
	Iterator<?> idsBackwards();

	/**
	 * Returns the compression ratio of the container.
	 *
	 * @return the current compression ratio
	 */
	double getCompressionRatio();

	/**
	 * Returns the allocated space in bytes.
	 *
	 * @return the allocated space in bytes
	 */
	long getAllocatedSpace();

	/**
	 * Allocates the given amount of (additional) space.
	 *
	 * @param space
	 *            the amount of additional space to be allocated
	 */
	void allocateSpace(long space);
}
