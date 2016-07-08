package bloomfilter.tests;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import bloomfilter.impl.BloomFilterFactoryImpl;

public class BloomFilterTests {
	// Max number of entries
	private static final int BLOOM_CAPACITY = 100000;
	// False positive error rate
	private static final double BLOOM_ERROR_RATE = 0.005;
	
	private static final String[] keysTemplates = new String[] {
														"linkPost:1005054",
											            "linkPost:7878989",
											            "linkPost:1005053",
											            "linkPost:2005054",
											            "linkPost:9015054",
											            "Post:2005054",
											            "Post:3005564",
											            "Post:1405555",
											            "Post:1455054",
											            "Post:1005558"
													};
	
	private static List<String> allKeys;
	
	@BeforeClass
	public static void testClassSetup(){
		allKeys = new ArrayList<String>();
		for (int i=0; i < BLOOM_CAPACITY/keysTemplates.length; i++){
			for (String t : keysTemplates){				
				allKeys.add(t+String.valueOf(i));
			}			
		}
	}
	
	@Test
	public void shouldLoadBloomFilterFromBytesFile() throws IOException{			
		byte[] dumpData = readBytesFromFileResource("/bloomfilter/bloomfilter.bin");
				
		BloomFilter bloomFilter = new BloomFilterFactoryImpl().load(dumpData);
				
		checkBloomFilterContains(bloomFilter);
	}
	
	@Test
	public void shouldLoadBloomFilterFromBytes64File() throws IOException{			
		byte[] dumpData = readBytesFromFileResource("/bloomfilter/bloomfilter.base64");
				
		BloomFilter bloomFilter = new BloomFilterFactoryImpl().loadFromBase64(dumpData);
				
		checkBloomFilterContains(bloomFilter);
	}
	
	@Test
	public void shouldDumpAndLoadBloomFilter() throws IOException{			
		BloomFilter bloomFilter = new BloomFilterFactoryImpl().create(BLOOM_CAPACITY, BLOOM_ERROR_RATE);
		
		for(String key : allKeys){
			bloomFilter.add(key);			
		}
		
		byte[] dumpData = bloomFilter.dump(false);
				
		BloomFilter bloomFilter2 = new BloomFilterFactoryImpl().load(dumpData);
		
		assertTrue("Dumped and Loaded Bloom Filter are not the same", bloomFilter.equals(bloomFilter2));
				
		checkBloomFilterContains(bloomFilter2);
	}
	
	@Test
    public void shouldDumpGzippedAndLoadBloomFilter() throws IOException{          
        BloomFilter bloomFilter = new BloomFilterFactoryImpl().create(BLOOM_CAPACITY, BLOOM_ERROR_RATE);
        
        for(String key : allKeys){
            bloomFilter.add(key);           
        }
        
        byte[] dumpData = bloomFilter.dump(true);
                
        BloomFilter bloomFilter2 = new BloomFilterFactoryImpl().load(dumpData);
        
        assertTrue("Gzipped Dumped and Loaded Bloom Filter are not the same", bloomFilter.equals(bloomFilter2));
                
        checkBloomFilterContains(bloomFilter2);
    }
	
	@Test
	public void shouldDumpAndLoadBase64BloomFilter() throws IOException{			
		BloomFilter bloomFilter = new BloomFilterFactoryImpl().create(BLOOM_CAPACITY, BLOOM_ERROR_RATE);
		
		for(String key : allKeys){
			bloomFilter.add(key);			
		}
		
		byte[] dumpData = bloomFilter.dumpToBase64(false);
				
		BloomFilter bloomFilter2 = new BloomFilterFactoryImpl().loadFromBase64(dumpData);
		
		assertTrue("Dumped and Loaded Bloom Filter are not the same", bloomFilter.equals(bloomFilter2));
				
		checkBloomFilterContains(bloomFilter2);
	}
	
	@Test
    public void shouldDumpGzippedAndLoadBase64BloomFilter() throws IOException{            
        BloomFilter bloomFilter = new BloomFilterFactoryImpl().create(BLOOM_CAPACITY, BLOOM_ERROR_RATE);
        
        for(String key : allKeys){
            bloomFilter.add(key);           
        }
        
        byte[] dumpData = bloomFilter.dumpToBase64(true);
                
        BloomFilter bloomFilter2 = new BloomFilterFactoryImpl().loadFromBase64(dumpData);
        
        assertTrue("Gzipped Dumped and Loaded Bloom Filter are not the same", bloomFilter.equals(bloomFilter2));
                
        checkBloomFilterContains(bloomFilter2);
    }
	
	private void checkBloomFilterContains(BloomFilter bloomFilter){
		int errors = 0;
		for(String key : allKeys){
			if (!bloomFilter.contains(key)){
				errors += 1;
			}
		}
		assertEquals("Some included keys were not found in the Bloom Filter (false negatives)", 0, errors);
		
		errors = 0;
		for(String key : allKeys){
			if (bloomFilter.contains(key+"DOES NOT EXISTS")){
				errors += 1;
			}
		}
		double errorRate = (double) errors / BLOOM_CAPACITY;
		
		assertThat("Too many keys not included were found in the Bloom Filter (false positives), above the double expected error rate of "+BLOOM_ERROR_RATE, 
				   errorRate, lessThan(BLOOM_ERROR_RATE*2));
	}
	
	
	@Test
	public void shouldUnionBloomFilters(){
		String[] bloomFilter2Keys = new String[] { "MYKEY1", "MYKEY2", "MYKEY3" };
		
		// Building and testing BloomFilter1
		BloomFilter bloomFilter1 = new BloomFilterFactoryImpl().create(BLOOM_CAPACITY, BLOOM_ERROR_RATE);		
		for(String key : keysTemplates){
			bloomFilter1.add(key);			
		}

		if (!bloomFilterContainsOrNot(bloomFilter1, keysTemplates, true)){
			fail("BloomFilter #1 should contain its keys");
		}
		
		if (!bloomFilterContainsOrNot(bloomFilter1, bloomFilter2Keys, false)){
			fail("BloomFilter #1 should not contain BloomFilter #2 keys");
		}
				
		// Building and testing BloomFilter2
		BloomFilter bloomFilter2 = new BloomFilterFactoryImpl().create(BLOOM_CAPACITY, BLOOM_ERROR_RATE);	
		for(String key : bloomFilter2Keys){
			bloomFilter2.add(key);			
		}
		
		if (!bloomFilterContainsOrNot(bloomFilter2, bloomFilter2Keys, true)){
			fail("BloomFilter #2 should contain its keys");
		}
		
		if (!bloomFilterContainsOrNot(bloomFilter2, keysTemplates, false)){
			fail("BloomFilter #2 should not contain BloomFilter #1 keys");
		}
		
		BloomFilter unitedBloomFilter = bloomFilter1.union(bloomFilter2);
		
		if (!bloomFilterContainsOrNot(unitedBloomFilter, keysTemplates, true)){
			fail("BloomFilter United should contain BloomFilter #1 keys");
		}
		
		if (!bloomFilterContainsOrNot(unitedBloomFilter, bloomFilter2Keys, true)){
			fail("BloomFilter United should contain BloomFilter #2 keys");
		}
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void shouldRaiseExceptionUnionBloomFiltersDiffCapacity() throws IOException{			
		BloomFilter bloomFilter1 = new BloomFilterFactoryImpl().create(BLOOM_CAPACITY, BLOOM_ERROR_RATE);
		BloomFilter bloomFilter2 = new BloomFilterFactoryImpl().create(100, BLOOM_ERROR_RATE);
		bloomFilter1.union(bloomFilter2);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void shouldRaiseExceptionUnionBloomFiltersDiffErrorRate() throws IOException{			
		BloomFilter bloomFilter1 = new BloomFilterFactoryImpl().create(BLOOM_CAPACITY, BLOOM_ERROR_RATE);
		BloomFilter bloomFilter2 = new BloomFilterFactoryImpl().create(BLOOM_CAPACITY, 0.1);
		bloomFilter1.union(bloomFilter2);
	}
	
	private boolean bloomFilterContainsOrNot(BloomFilter bf, String[] keys, boolean shouldContain){
		for(String key : keys){
			if (bf.contains(key) != shouldContain){
				return false;
			}
		}
		return true;
	}
	
	private byte[] readBytesFromFileResource(String resourcePath) throws IOException{
		URL tfidf_test_resource = this.getClass().getResource(resourcePath);		
		InputStream stream = tfidf_test_resource.openStream();
		ByteArrayOutputStream buffer = new ByteArrayOutputStream();

		int nRead;
		byte[] bufferData = new byte[16384];
		while ((nRead = stream.read(bufferData, 0, bufferData.length)) != -1) {
			buffer.write(bufferData, 0, nRead);
		}
		buffer.flush();

        return buffer.toByteArray();
	}

}
