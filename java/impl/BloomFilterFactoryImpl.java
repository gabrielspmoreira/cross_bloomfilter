package cross_bloomfilter.bloomfilter.impl;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import com.google.api.client.util.Base64;

import cross_bloomfilter.bloomfilter.BloomFilter;
import cross_bloomfilter.bloomfilter.BloomFilterFactory;

public class BloomFilterFactoryImpl implements BloomFilterFactory{
	
	@Override
	public BloomFilter create(int capacity, double error){
		return new BloomFilterImpl(capacity, error);
	}
	
	/**
     * Loads a Bloom Filter from a dump of bytes
     * @param bytes A dump of bytes
     * @return A Bloom Filter instance
	 * @throws IOException 
     */
    @Override
    public BloomFilter load(byte[] dump) throws IOException {
        ByteBuffer bb = ByteBuffer.wrap(dump);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        // 9 bytes is the size of the header
        short checksum = bb.getShort();
        byte gzipped = bb.get();
        double errorRate = 1.0 / bb.getShort();
        int cardinality = bb.getInt();
        byte[] data = Arrays.copyOfRange(dump, bb.position(), dump.length);

        if (BloomFilterCommons.computeChecksum(data) != checksum)
            throw new InvalidObjectException("Bad checksum");                 
        
        // Uncompressing data if gzipped
        if (gzipped == 1) {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            GZIPInputStream gzipIn = new GZIPInputStream(bais);
            DataInputStream dis = new DataInputStream(gzipIn);
            int dataBytesSize = BloomFilterImpl.getDataBytesSize(cardinality, errorRate);
            byte[] unzippedData = new byte[dataBytesSize];
            dis.readFully(unzippedData);
            data = unzippedData;
            dis.close();
        }        
          
        return new BloomFilterImpl(data, cardinality, errorRate);
    }
    
    /**
     * Loads a Bloom Filter from a dump of base64 encoded bytes
     * @param dump A dump of bytes
     * @return A Bloom Filter instance
     * @throws InvalidObjectException
     */
    @Override
    public BloomFilter loadFromBase64(byte[] dump) throws IOException {
    	byte[] decodedBytes =  Base64.decodeBase64(dump);
    	return load(decodedBytes);
    }

}
