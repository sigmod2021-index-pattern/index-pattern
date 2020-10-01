package xxl.core.collections.containers.compression.buffer;

import xxl.core.io.raw.RawAccess;

/**
 *
 */
public interface BlockBuffer {

	/**
	 * Sets the raw access
	 *
	 * @param raf
	 *            the raw access
	 */
	void setRawAccess(RawAccess raf);

	/**
	 * Returns a block.
	 *
	 * @param b
	 *            the byte array the block should be loaded into
	 * @param blockNumber
	 *            the number of the block to be loaded
	 * @param tlbLeaf
	 *            true if te block is a tlb leaf, false otherwise
	 */
	void getBlock(byte[] b, long blockNumber, boolean tlbLeaf);

	/**
	 * Replaces the given block in the buffer.
	 *
	 * @param blockNumber
	 *            the block number
	 * @param block
	 *            the block
	 */
	void replace(long blockNumber, byte[] block);

	/**
	 * Reads out a TLB entry.
	 *
	 * @param b
	 *            the target array
	 * @param blockNumber
	 *            the block number of the TLB block
	 * @param level
	 *            the level of the TLB entry
	 * @param levelIndex
	 *            the index of the entry within the TLB level
	 */
	void getTLBEntry(byte[] b, long blockNumber, int level, int levelIndex);

	/**
	 * Places the given TLB block in the buffer, if necessary.
	 *
	 * @param block
	 *            the serialized TLB entry
	 * @param level
	 *            the level of the TLB block
	 * @param levelIndex
	 *            the index of the TLB entry within the level
	 */
	void replaceTLB(byte[] block, int level, int levelIndex);

	/**
	 * Returns the current size of the buffer.
	 *
	 * @return the buffer`s size
	 */
	int getSize();
	
	
	boolean isOutOfOrder();
	
	void setOutOfOrder(boolean value);

	String logState();

}
