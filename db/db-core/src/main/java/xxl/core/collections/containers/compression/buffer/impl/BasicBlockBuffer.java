package xxl.core.collections.containers.compression.buffer.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xxl.core.collections.containers.compression.buffer.BlockBuffer;
import xxl.core.io.raw.RawAccess;

/**
 * A circular buffer for sequential reading
 */
public class BasicBlockBuffer implements BlockBuffer {

	/**
	 * The logger
	 */
	private final Logger		logger					= LoggerFactory
		.getLogger(BasicBlockBuffer.class);

	/**
	 * The granularity for read buffer updates
	 */
	private static final int	blockUpdateGranularity	= 1;

	private final int			blockSize;
	private final byte[][]		blocks;

	private final byte[][]		tlbBuffer;
	protected int				tlbLevel;
	private int					tlbBufferPosition;

	private long				firstBufferedBlock;
	private long				lastBlockRead;
	private int					head;
	private int					tail;
	private RawAccess			raf;
	/** Flag indicating if an out-of-order insertion is active */
	private boolean			outOfOrder;

	public BasicBlockBuffer(final int bufferSize, final int tlbBufferSize, final int blockSize, final RawAccess raf) {
		this.blockSize = blockSize;
		this.blocks = new byte[bufferSize][];
		this.tlbBuffer = new byte[tlbBufferSize][];
		this.raf = raf;
		this.firstBufferedBlock = -(bufferSize + 1);
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public void setRawAccess(final RawAccess raf) {
		this.raf = raf;
	}

	/**
	 * Initializes the buffer with the given block number
	 *
	 * @param blockNumber
	 */
	private void init(final long blockNumber) {
		logger.trace("Initializing at block " + blockNumber);
		this.firstBufferedBlock = (int) Math.max(0, blockNumber - this.blocks.length + 2);
		this.head = 0;
		for (long i = this.firstBufferedBlock; i <= blockNumber + 1 && i < this.raf.getNumSectors(); i++) {
			final byte[] block = new byte[this.blockSize];
			this.raf.read(block, i);
			logger.trace("Storing block " + i + " to buffer position " + ((int) (i - this.firstBufferedBlock)));
			this.blocks[(int) (i - this.firstBufferedBlock)] = block;
			this.head = (int) (i - this.firstBufferedBlock);
		}
		this.tail = 0;
		this.lastBlockRead = this.firstBufferedBlock;
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public void getBlock(final byte[] b, final long blockNumber, final boolean tlbLeaf) {

		if ( !this.outOfOrder && tlbLeaf && (this.head == this.tail || blockNumber < this.firstBufferedBlock ||
			blockNumber >= this.firstBufferedBlock + getSize()) ) { // Loading a TLB block => buffer all preceding entries
			if ( blockNumber != this.firstBufferedBlock + getSize() ) {
				init(blockNumber);
			}
		}

		if ( blockNumber < this.firstBufferedBlock || blockNumber >= this.firstBufferedBlock + getSize() ) { // Out-of-order block => load without buffer
			this.raf.read(b, blockNumber);
		}
		else {
			final int realBlockNumber = (int) (blockNumber - this.firstBufferedBlock);
			final int blockIndex = (this.tail + realBlockNumber) % this.blocks.length;
			logger.trace("Loading block " + blockNumber + " from buffer position " + blockIndex);
			final byte[] result = this.blocks[blockIndex];
			System.arraycopy(result, 0, b, 0, b.length);
			if ( !this.outOfOrder ) {
				updateBuffer(blockNumber);
			}
		}
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public void replace(final long blockNumber, final byte[] block) {
		if ( blockNumber >= this.firstBufferedBlock && blockNumber < this.firstBufferedBlock + getSize() ) {
			final int realBlockNumber = (int) (blockNumber - this.firstBufferedBlock);
			final int blockIndex = (this.tail + realBlockNumber) % this.blocks.length;
			logger.trace("Replacing block " + blockNumber + " in buffer at position " + blockIndex);
			System.arraycopy(block, 0, this.blocks[blockIndex], 0, block.length);
		}
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public void getTLBEntry(final byte[] b, final long blockNumber, final int level, final int levelIndex) {
		if ( this.outOfOrder && ((level == this.tlbLevel && levelIndex < this.tlbBufferPosition) ||
			(this.tlbLevel > 0 && level == this.tlbLevel - 1 && levelIndex >= this.tlbBufferPosition &&
				this.tlbBufferPosition < 1)) ) {
			System.arraycopy(this.tlbBuffer[levelIndex], 0, b, 0, b.length);
		}
		else {
			getBlock(b, blockNumber, level == 0);
		}
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public void replaceTLB(final byte[] block, final int level, final int levelIndex) {
		if ( level == this.tlbLevel ) {// The current level
			this.tlbBuffer[levelIndex] = block;
			if ( levelIndex >= this.tlbBufferPosition ) {
				if ( levelIndex - this.tlbBufferPosition > 1 )
					throw new RuntimeException("Error here! The buffer skipped levels"); // Check, that levelIndex == 0 for level changes!
				this.tlbBufferPosition = levelIndex + 1;
			}
		}
		else if ( level == this.tlbLevel + 1 ) { // The next level
			this.tlbBuffer[levelIndex] = block;
			if ( levelIndex != 0 )
				throw new RuntimeException("Error here!!"); // Check, that levelIndex == 0 for level changes!
			this.tlbBufferPosition = levelIndex + 1;
			this.tlbLevel = level;
		}
	}

	/**
	 * Updates the buffer.
	 *
	 * @param blockNumber
	 *            the previously read block number
	 */
	private void updateBuffer(final long blockNumber) {
		final int buffSize = getSize();
		if ( blockNumber >= this.firstBufferedBlock && blockNumber < this.firstBufferedBlock + getSize() ) {
			for (int i = 0; i < blockUpdateGranularity; i++) {
				final long blockToLoad = this.firstBufferedBlock + buffSize;
				if ( blockToLoad >= this.raf.getNumSectors() )
					return;
				this.tail = (this.tail + 1) % this.blocks.length;
				this.head = (this.head + 1) % this.blocks.length;
				if ( this.blocks[this.head] == null ) {
					this.blocks[this.head] = new byte[this.blockSize];
				}
				logger.trace("Storing block " + blockToLoad + " to buffer position " + this.head);
				this.raf.read(this.blocks[this.head], blockToLoad);
				this.firstBufferedBlock++;
				this.lastBlockRead++;
			}
		}
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public int getSize() {
		if ( this.head < this.tail )
			return this.blocks.length + this.head - this.tail + 1;
		else
			return this.head - this.tail + 1;
	}
	
	
	/**
	 * @return the outOfOrder
	 */
	public boolean isOutOfOrder() {
		return this.outOfOrder;
	}

	
	/**
	 * @param outOfOrder the outOfOrder to set
	 */
	public void setOutOfOrder(boolean outOfOrder) {
		this.outOfOrder = outOfOrder;
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public String logState() { 
		StringBuilder sb = new StringBuilder();
		sb.append("Block buffer");
		sb.append("\r\n");
		sb.append("-----------------------------");
		sb.append("\r\n");
		sb.append("outOfOrder: " + outOfOrder);
		sb.append("\r\n");
		sb.append("firstBufferedBlock: " + firstBufferedBlock);
		sb.append("\r\n");
		sb.append("lastBlockRead: " + lastBlockRead);
		sb.append("\r\n");
		sb.append("head: " + head);
		sb.append("\r\n");
		sb.append("tail: " + tail);
		sb.append("\r\n");
		sb.append("size: " + getSize());
		sb.append("\r\n");
		return sb.toString();
	}
}
