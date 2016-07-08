## cross_bloomfilter

_cross_bloomfilter_ - Pure Python 3 and Java Bloom Filter compatible implementations
(https://en.wikipedia.org/wiki/Bloom_filter).

## What's a Bloom Filter?
A Bloom filter is space-efficient Probabilistic Data Structure used to test whether an element is a member of a large set. 
It can have false positives but never false negatives, which means a query returns either "possibly in set" or "definitely not in set".

You can tune a Bloom filter to the desired error rate, it's basically a tradeoff between size and accuracy (See example: http://hur.st/bloomfilter). For example, a filter for about 100,000 keys, configured with a 0.5% false positive error rate can be compressed to 137 KB bytes, which is about 8% of the whole list of keys (with average length of 18 characters) (1,788 KB). With 5% error rate it can be compressed to just 18 KB.

## Why Cross Language compatible implementation?
The motivation for this implementation was the need to populate Bloom Filters in Python, serialize its bytes and load it within a Java Web Application.  
You can dump/load the Bloom Filter Bytes from/to Java or Python 3.    
p.s. the Java implementation was inspired in [inbloom](http://github.com/EverythingMe/inbloom) project.


## Dump headers
InBloom provides utilities for serializing / deserializing Bloom filters so they can be sent over the network. It support dumping options like gzip and base64 formats.
Since when you create a Bloom filter, you need to initialize it with parameters of expected cardinality and false positive rates. Those parameters are stored as a header when serializing the filter. It uses a 16 bit checksum as part of the header.

### Serialized filter structure:

| Field        | Type            | bits |
| ------------- |:-------------:| -----:|
| checksum      | ushort | 16 |
| gzipped		| byte | 8 |
| errorRate (1/N)| ushort | 16 |
| capacity   | int     |   32 |
| data          | byte[]  | ? |


### Example Usage

#### Python
```python
from bloom_filter import BloomFilter

#Populating Bloom Filter
bf = BloomFilter(capacity=100000, error=0.005)        
bf.add('key_abc')      
bf.add('key_def') 

#Check if keys are in the set
assert('key_abc' in bf)    
assert('key_def' in bf)    
assert('key_ghi' not in bf)    

#Serializing and deserializing Bloom Filter
dump_bytes = bloom_filter.dump(gzipped=True)                
bf2 = BloomFilter.load(dump_bytes)

#Check if keys are in the set
assert('key_abc' in bf2)    
assert('key_def' in bf2)    
assert('key_ghi' not in bf2)   

# Serializing to base64 (string) for storage in database or sending over an HTTP API
dump_base64 = bloom_filter.dump_to_base64_str(gzipped=True);

```



#### Java
```java
import cross_bloomfilter.bloomfilter.impl.BloomFilter;
import cross_bloomfilter.bloomfilter.impl.BloomFilterFactory;

// Popularing Bloom Filter
BloomFilter bloomFilter = new BloomFilterFactoryImpl().create(100000, 0.005);
bloomFilter.add('key_abc');
bloomFilter.add('key_def');

// Check if keys are in the set
assert(bloomFilter.contains('key_abc'))
assert(bloomFilter.contains('key_def'))
assert(!bloomFilter.contains('key_ghi'))

// Serializing and deserializing
byte[] dumpData = bloomFilter.dump(true);		
BloomFilter bloomFilter2 = new BloomFilterFactoryImpl().load(dumpData);

// Check if keys are in the set
assert(bloomFilter2.contains('key_abc'))
assert(bloomFilter2.contains('key_def'))
assert(!bloomFilter2.contains('key_ghi'))

// Serializing to base64 (string) for storage in database or sending over an HTTP API
String dumpBase64 = bloomFilter.dumpToBase64(true);
```