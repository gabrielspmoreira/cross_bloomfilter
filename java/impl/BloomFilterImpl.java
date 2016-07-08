package cross_bloomfilter.bloomfilter.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Objects;
import java.util.zip.GZIPOutputStream;

import cross_bloomfilter.BloomFilter;
import com.google.api.client.util.Base64;
import com.google.common.hash.Hashing;

/**
 * Implements a Bloom Filter with dump and load functionalities.
 * WARNING: Do not change constants or functions like checksum and hash because the dumps an calculation are
 *  compatible with a Python implementation (bloom_filter.py) 
 * @author gabrielpm
 *
 */
public class BloomFilterImpl implements BloomFilter, Serializable {
	
	private static final long serialVersionUID = 1706701879289640889L;
	
	protected int capacity; 
    protected double error;
    protected int bits;
    protected int bytes;
    protected int hashes;    
    protected byte[] bf; //Bit array
    
    protected final static double MIN_ERROR_PRECISION = 1.0 / (double) Short.MAX_VALUE;
    
    protected int DUMP_HEADER_BYTES_SIZE = 9;
	
    /**
     * Instantiates a Bloom Filter
     * @param capacity Estimated number of entries. If it is added more entries than this number, the error will grow up well above error parameter
     * @param error The desired rate of false positives errors. P.s. There is no false negative in Bloom Filter.
     */
    protected BloomFilterImpl(int capacity, double error)
    {
        this(null, capacity, error);
    }
	
	/**
     * Instantiates a Bloom Filter
     * @param data The data bytes of the bloom filter 
     * @param capacity Estimated number of entries. If it is added more entries than this number, the error will grow up well above error parameter
     * @param error The desired rate of false positives errors. P.s. There is no false negative in Bloom Filter.
     */
	protected BloomFilterImpl(byte[] data, int capacity, double error) throws RuntimeException
    {
		initialize(data, capacity, error);
    }
	
	protected void initialize(byte[] data, int capacity, double error) throws RuntimeException
    {
		if (capacity < 1 || ( ( 1.0 <= error ) || ( error <= MIN_ERROR_PRECISION ) ) ) {
            throw new RuntimeException("Invalid params for bloom filter");
        }

        this.capacity = capacity;
        this.error = error;
        this.bits = getDataBitsSize(capacity, error);
        this.bytes = getDataBytesSize(capacity, error);
        this.hashes = (int) (((float) this.bits * Math.log(2) / this.capacity) + 1);
        
        if (data != null) {
            if (bytes != data.length) {
                throw new RuntimeException(String.format("Expected %d bytes, got %d", bytes, data.length));
            }
            bf = data;
        } else {
            bf = new byte[bytes];
        }
    }
	
	public static int getDataBitsSize(int capacity, double error){
	    int bits = (int) (-capacity * Math.log(error) / Math.pow(Math.log(2),2) + 1);
	    return bits;
	}
	
	public static int getDataBytesSize(int capacity, double error){
	    int bits = getDataBitsSize(capacity, error);
        int bytes = (bits / 8) + (bits % 8 != 0 ? 1 : 0);
        return bytes;
	}
	
	protected static long unsigned(int i) {
        return i & 0xffffffffl;
    }


	protected boolean checkAdd(String key,boolean add) {

        int hits = 0;
        long a = unsigned(Hashing.murmur3_32(42).hashString(key, Charset.defaultCharset()).asInt());
        long b = unsigned(Hashing.murmur3_32((int)a).hashString(key, Charset.defaultCharset()).asInt());

        for (int i = 0; i < hashes; i++) {
            long theBit = unsigned ((int)(a + i*b)) % bits;
            long theByte = theBit >> 3; //Converting bits to bytes dividing by 8

            byte c = bf[(int)theByte];        
            byte mask = (byte)(1 << (theBit % 8));

            if ((c & mask) != 0) {
                hits++;
            } else {
                if (add) {
                    bf[(int)theByte] = (byte)(c | mask);
                }
            }
        }

        return hits == hashes;
    }


    /**
     * Verifies if the Bloom Filter contains a key.
     * @param key
     * @return If return is False, the key is certainly not in the Bloom Filter. If return is True, the key is probably in the set, with an error controlled by the error attribute
     */
	@Override
    public boolean contains(String key)  {
        return checkAdd(key, false);
    }


    /**
     * Adds a key to the Bloom Filter
     * @param key
     * @return
     */
	@Override
	public boolean add(String key) {
        return checkAdd(key, true);
	
    }
	
    /**
     * Dumps the Bloom Filter to bytes
     * @throws IOException 
     */
	@Override
	public byte[] dump(boolean gzipped) throws IOException {
        byte[] data = this.bf;
        
        if (gzipped){
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            GZIPOutputStream gzipOut = new GZIPOutputStream(baos);
            gzipOut.write(data);
            gzipOut.close();
            byte[] zippedData = baos.toByteArray();           
            data = zippedData;            
        }
        
        byte[] bytes = new byte[DUMP_HEADER_BYTES_SIZE + data.length];
        
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.order(ByteOrder.LITTLE_ENDIAN);

        short checksum = BloomFilterCommons.computeChecksum(data);
        byte gzippedByte = (byte) (gzipped ? 1 : 0); 
        short inverseErrorRate = (short) (1.0 / this.error);
        
        bb.putShort(checksum);
        bb.put(gzippedByte);
        bb.putShort(inverseErrorRate);
        bb.putInt(this.capacity);
        bb.put(data);

        return bytes;
    }
    
    /**
     * Dumps the Bloom Filter to base64 encoded bytes
     * @return
     * @throws IOException 
     */
	@Override
	public byte[] dumpToBase64(boolean gzipped) throws IOException {
    	byte[] dumpBytes = dump(gzipped);
    	return Base64.encodeBase64(dumpBytes);
    }
    
    
    @Override
    public int hashCode()
    {
       return Objects.hash(
          this.capacity, this.error, this.bits, this.bytes, this.hashes, BloomFilterCommons.computeChecksum(this.bf) );
    }
    
    @Override
    public boolean equals(Object obj) 
    {
       if (obj == null)
       {
          return false;
       }
       if (getClass() != obj.getClass())
       {
          return false;
       }
       final BloomFilterImpl other = (BloomFilterImpl) obj;
       boolean result = Objects.equals(this.capacity, other.capacity)
              && Objects.equals(this.error, other.error)
              && Objects.equals(this.bits, other.bits)
              && Objects.equals(this.bytes, other.bytes)
              && Objects.equals(this.hashes, other.hashes)
              && Arrays.equals(this.bf,other.bf);
       return result;
    }    
    
    @Override
    public BloomFilter union(BloomFilter other){
    	if (this.getCapacity() != other.getCapacity()){
    		throw new IllegalArgumentException("Bloom filter capacities must be equal!");
    	}
    	if (this.getErrorRate() != other.getErrorRate()){
    		throw new IllegalArgumentException("Bloom filter error rates must be equal!");
    	}
    	
    	byte[] thisBytesDate = this.getDataBytes();
    	byte[] otherBytesDate = other.getDataBytes();
    	if (thisBytesDate.length != otherBytesDate.length){
    		throw new IllegalArgumentException("Bloom filter data bytes length must be equal!");
    	}
    	
    	int size = thisBytesDate.length;
    	byte[] newBytesData = new byte[size];
    	for (int i=0; i<size; i++){
    		newBytesData[i] = (byte) (thisBytesDate[i] | otherBytesDate[i]);
    	}
    	
    	return new BloomFilterImpl(newBytesData, this.getCapacity(), this.getErrorRate());
    }
    
    @Override
    public int getCapacity(){
    	return capacity;
    }
    
    @Override
    public double getErrorRate(){
    	return error;
    }
    
    @Override
    public byte[] getDataBytes(){
    	return bf;
    }
}