package cross_bloomfilter.bloomfilter;

import java.io.IOException;

public interface BloomFilter {
	boolean contains(String key);
	boolean add(String key);
	byte[] dump(boolean gzipped) throws IOException;
	byte[] dumpToBase64(boolean gzipped) throws IOException;
	BloomFilter union(BloomFilter other);
	int getCapacity();    
    double getErrorRate();    
    byte[] getDataBytes();
}
