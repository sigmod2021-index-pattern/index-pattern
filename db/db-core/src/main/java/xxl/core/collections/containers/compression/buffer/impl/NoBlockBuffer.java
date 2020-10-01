package xxl.core.collections.containers.compression.buffer.impl;

import xxl.core.collections.containers.compression.buffer.BlockBuffer;
import xxl.core.io.raw.RawAccess;


/**
 *
 */
public class NoBlockBuffer implements BlockBuffer {

	private RawAccess raf;
	
	private boolean ooo = false;
	
	/**
	 * Creates a new NoBlockBuffer instance
	 * @param raf
	 */
	public NoBlockBuffer(RawAccess raf) {
		this.raf = raf;
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public void setRawAccess(RawAccess raf) {
		this.raf = raf;
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public void getBlock(byte[] b, long blockNumber, boolean tlbLeaf) {
//		System.out.print(blockNumber + ", ");
		raf.read(b, blockNumber);
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public void replace(long blockNumber, byte[] block) {
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public void getTLBEntry(byte[] b, long blockNumber, int level, int levelIndex) {
		getBlock(b, blockNumber, false);
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public void replaceTLB(byte[] block, int level, int levelIndex) {
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public int getSize() {
		return 0;
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public boolean isOutOfOrder() {
		return ooo;
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public void setOutOfOrder(boolean value) {
		ooo = value;
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public String logState() {
		return "Empty BlockBuffer";
	}

}
