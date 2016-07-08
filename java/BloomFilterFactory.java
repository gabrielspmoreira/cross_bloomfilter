package cross_bloomfilter.bloomfilter;

import java.io.IOException;

public interface BloomFilterFactory {
	
	BloomFilter create(int capacity, double error);
	
	BloomFilter load(byte[] dump) throws IOException;
	
	BloomFilter loadFromBase64(byte[] dump) throws IOException;

}
