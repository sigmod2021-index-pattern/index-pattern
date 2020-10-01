package xxl.core.collections.containers.compression;

import java.io.DataInput;
import java.io.DataOutput;

import net.jpountz.lz4.LZ4Factory;

/**
 * Compressor implementation that uses the
 * <a href="http://fastcompression.blogspot.de/2011/05/lz4-explained.html">LZ4
 * library</a> for compression.
 *
 * @see Compressor
 */
public class LZ4Compressor implements Compressor {

	/**
	 * The compressor
	 */
	private final net.jpountz.lz4.LZ4Compressor			compressor;

	/**
	 * The decompressor
	 */
	private final net.jpountz.lz4.LZ4FastDecompressor	decompressor;

	/**
	 * Creates a new LZ4 compressor
	 */
	public LZ4Compressor() {
		this.compressor = LZ4Factory.fastestInstance().fastCompressor();
		this.decompressor = LZ4Factory.fastestInstance().fastDecompressor();
	}

	@Override
	public byte[] compress(final byte[] b) {
		final byte[] destination = new byte[b.length];
		final int size = this.compressor.compress(b, 0, b.length, destination, 0);

		final byte[] result = new byte[size];
		System.arraycopy(destination, 0, result, 0, size);
		return result;
	}

	@Override
	public byte[] decompress(final byte[] b, final int length) {
		final byte[] decompressed = new byte[length];
		this.decompressor.decompress(b, 0, decompressed, 0, length);
		return decompressed;
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public void writeParams(DataOutput out) {
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public void restoreParams(DataInput in) {
	}
}
