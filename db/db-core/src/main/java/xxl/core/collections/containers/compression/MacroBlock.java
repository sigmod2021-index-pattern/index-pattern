package xxl.core.collections.containers.compression;

/**
 * Abstraction of a macro block.
 *
 */
public interface MacroBlock {

	/**
	 * Adds a block to this macro block.
	 *
	 * @param block
	 *            the block
	 * @param blockSize
	 *            the total size of the block. This is important if the block
	 *            does not fit into this macro block. In that case, block only
	 *            contains the part for this macro block
	 */
	void add(byte[] block, int blockSize);

	/**
	 * Adds the remainder of the last block of the previous macro block to this
	 * one.
	 *
	 * @param blockOffset
	 *            the remainder of the last block
	 */
	void addOffset(byte[] blockOffset);

	/**
	 * Returns the free available space in this block. The amount of free space
	 * relates to the actual payload. Additional space for meta information is
	 * already considered. Spare is not considered as &quot;free&quot;.
	 *
	 * @return the free space in bytes
	 */
	int freeSpace();

	/**
	 * Serializes this macro block.
	 *
	 * @param b
	 *            the byte array to serialize the macro block into.
	 */
	void getBytes(byte[] b);

	/**
	 * Returns the entry at the given position within this macro block.
	 *
	 * @param id
	 *            the id (position) of the entry
	 * @return the bytes of the requested entry within this block and its total
	 *         size
	 */
	EntryResult getEntry(int id);

	/**
	 * Returns the offset of this macro block.
	 *
	 * @return the offset bytes of the previous block
	 */
	byte[] getOffset();

	/**
	 * Returns if the block at the given position is an overflow block
	 *
	 * @param id
	 *            the number of the block
	 * @return true, if the is an overflow block, false otherwise
	 */
	boolean isOverflowBlock(int id);

	/**
	 * Updates the block with the given id.
	 *
	 * @param id
	 *            the id of the block to be updated
	 * @param block
	 *            the block
	 * @param blockSize
	 *            the full size of the block
	 */
	void replace(int id, byte[] block, int blockSize);

	/**
	 * Sets the given entry as last entry. I.e., removes all entries after the
	 * given index!
	 *
	 * @param id
	 *            the id of the block to be set as last block
	 */
	void setToLast(int id);

	/**
	 * Updates the offset of the current block.
	 *
	 * @param overflow
	 *            the new offset
	 */
	void updateOverflow(byte[] overflow);

	/**
	 * Returns the number of blocks contained in this macro block.
	 *
	 * @return the number of contained entries
	 */
	int getCount();

	/**
	 * Returns the current physical size of this macro block.
	 *
	 * @return the current physical size
	 */
	int getLength();

	/**
	 * Returns the total amount of space available in the macro block. This
	 * includes the update space as well as the insert space!
	 *
	 * @return the free available space in bytes
	 */
	int updateSpace();

	/**
	 * Sets a reference entry at the given position (after an update).
	 *
	 * @param id
	 *            the id of the entry that should be replaced by a reference
	 *            entry
	 * @param reference
	 *            the reference entry
	 */
	void setReferenceEntry(int id, ReferenceEntry reference);

	/**
	 * Returns if the entry at the given position is a reference entry.
	 *
	 * @param id
	 *            the id of the requested entry
	 * @return true, if the entry is a reference entry, false otherwise
	 */
	boolean isReferenceEntry(int id);

	/**
	 * Returns the reference entry at the given position.
	 *
	 * @param id
	 *            the id of the requested entry
	 * @return the reference entry at the given position
	 */
	ReferenceEntry readReferenceEntry(int id);
}
