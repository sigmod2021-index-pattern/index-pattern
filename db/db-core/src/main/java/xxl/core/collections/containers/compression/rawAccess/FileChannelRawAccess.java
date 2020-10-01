package xxl.core.collections.containers.compression.rawAccess;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import com.sun.nio.file.ExtendedOpenOption;

import xxl.core.io.raw.RawAccess;
import xxl.core.io.raw.RawAccessException;

/**
 *
 */
public class FileChannelRawAccess implements RawAccess {
	
	private FileChannel channel;
	
	private final int sectorSize;
	
	private long sectorCount;
	
	private final OpenOption[] openOptions;
	
	/**
	 * Creates a new FileChannelRawAccess instance
	 */
	public FileChannelRawAccess(int sectorSize, boolean directIO) {
		this.sectorSize = sectorSize;
		this.openOptions = directIO  ? new OpenOption[] { StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE, ExtendedOpenOption.DIRECT } : 
			  						   new OpenOption[] { StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE };
	}
	

	/**
	 * @{inheritDoc}
	 */
	@Override
	public void open(String filename) throws RawAccessException {
		try {
			this.channel = FileChannel.open(Paths.get(filename), openOptions);
			this.sectorCount = (this.channel.size() / this.sectorSize);
		}
		catch (IOException e) {
			throw new RawAccessException("Could not open file: " + e.getMessage());
		}
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public void close() throws RawAccessException {
		if ( this.channel == null )
			return;
		
		try {
			this.channel.close();
		}
		catch (IOException e) {
			throw new RawAccessException("Could not close file channel: " + e.getMessage());
		}
		this.channel = null;
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public void write(byte[] block, long sector) throws RawAccessException {
		assert block.length % sectorSize == 0;
		long pos = sector * sectorSize;
		ByteBuffer out = ByteBuffer.wrap(block);
		while ( out.hasRemaining() ) {
			try {
				pos += channel.write(out, pos);
			}
			catch (IOException e) {
				throw new RawAccessException("Could not write to file: " + e.getMessage());
			}
		}
		this.sectorCount = Math.max(this.sectorCount, sector + block.length/sectorSize);
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public void read(byte[] block, long sector) throws RawAccessException {
		assert block.length % sectorSize == 0;
		long pos = sector * sectorSize;
		ByteBuffer in = ByteBuffer.wrap(block);
		int read = 0;
		while ( read >= 0 && in.hasRemaining() ) {
			try {
				pos += read;
				read = channel.read(in, pos);
			}
			catch (IOException e) {
				throw new RawAccessException("Could not write to file: " + e.getMessage());
			}
		}
		if ( in.hasRemaining() )
			throw new RawAccessException("File ended before sector " + sector + " could be fully read");
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public long getNumSectors() {
		if ( channel == null || !channel.isOpen() )
			throw new IllegalStateException("Cannot determine number of sectors on closed file channel");
		return sectorCount;
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public int getSectorSize() {
		return sectorSize;
	}

}
