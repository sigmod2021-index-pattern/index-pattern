package xxl.core.collections.containers.compression;

/**
 * Encapsulates the result of the look-up of an (compressed) block. Compressed
 * blocks may span on two macro blocks.
 *
 */
public class EntryResult {

	/** The compressed block */
	public byte[]	array;
	/** The total length of the block */
	public int		length;

	/**
	 * Creates a new entry result.
	 *
	 * @param array
	 *            the compressed block
	 * @param length
	 *            the total size of the block
	 */
	public EntryResult(final byte[] array, final int length) {
		this.array = array;
		this.length = length;
	}
}
