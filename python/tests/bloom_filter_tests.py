#!/usr/bin/python
# -- coding: utf-8 --

import sys
sys.path.append("..")
from bloom_filter import BloomFilter

import unittest

class BloomFilterTest(unittest.TestCase):

    #Max number of entries
    BLOOM_CAPACITY = 100000
    #False positive error rate
    BLOOM_ERROR_RATE = 0.005

    BF_DUMP_FILE = "bloomfilter.bin"
    BF_DUMP_FILE_BASE_64 = "bloomfilter.base64"

    key_templates = ["linkPost:1005054",
                     "linkPost:7878989",
                     "linkPost:1005053",
                     "linkPost:2005054",
                     "linkPost:9015054",
                     "Post:2005054",
                     "Post:3005564",
                     "Post:1405555",
                     "Post:1455054",
                     "Post:1005558"]

    all_keys = []
    
    @classmethod
    def setUpClass(cls):
        for i in range(int(cls.BLOOM_CAPACITY / len(cls.key_templates))):
            for key in cls.key_templates:
                cls.all_keys.append(key+str(i))

    def setUp(self):
        pass

    def testDumpAndLoadBloomFilter(self):
        bloom_filter = BloomFilter(self.BLOOM_CAPACITY, self.BLOOM_ERROR_RATE)        
        for key in self.all_keys:
            bloom_filter.add(key)           
        
        dump_bytes = bloom_filter.dump(gzipped=False);                
        bloom_filter2 = BloomFilter.load(dump_bytes)
        
        self.assertEqual(bloom_filter, bloom_filter2)                
        self.check_contains(bloom_filter2)

    def testDumpGzippedAndLoadBloomFilter(self):
        bloom_filter = BloomFilter(self.BLOOM_CAPACITY, self.BLOOM_ERROR_RATE)        
        for key in self.all_keys:
            bloom_filter.add(key)           
        
        dump_bytes = bloom_filter.dump(gzipped=True);                
        bloom_filter2 = BloomFilter.load(dump_bytes)
        
        self.assertEqual(bloom_filter, bloom_filter2)                
        self.check_contains(bloom_filter2)

    #    with open(BloomFilterTest.BF_DUMP_FILE, 'wb') as f:
    #        f.write(dump_bytes)


    def testDumpAndLoadBase64BloomFilter(self):
        bloom_filter = BloomFilter(self.BLOOM_CAPACITY, self.BLOOM_ERROR_RATE)        
        for key in self.all_keys:
            bloom_filter.add(key);           
        
        dump_str = bloom_filter.dump_to_base64_str(gzipped=True);                
        bloom_filter2 = BloomFilter.load_from_base64_str(dump_str);
        
        self.assertEqual(bloom_filter, bloom_filter2)                
        self.check_contains(bloom_filter2)

        #with open(BloomFilterTest.BF_DUMP_FILE_BASE_64, 'w') as f:
        #    f.write(dump_str)

    def testLoadBloomFilterFromFile(self):
        with open(self.BF_DUMP_FILE, "rb") as f:
            dump_bytes = f.read()
        bloom_filter = BloomFilter.load(dump_bytes)
        self.check_contains(bloom_filter)

    def testLoadBloomFilterFromBase64File(self):
        with open(self.BF_DUMP_FILE_BASE_64, "r") as f:
            dump_str = f.read()
        bloom_filter = BloomFilter.load_from_base64_str(dump_str)
        self.check_contains(bloom_filter)

    def check_contains(self, bloom_filter):
        errors = 0
        for key in self.all_keys:
            if key not in bloom_filter:
                errors += 1
        self.assertEqual(errors, 0, msg="Some included keys were not found in the Bloom Filter (false negatives)")

        errors = 0
        for key in self.all_keys:
            if (key+"DOES NOT EXISTS") in bloom_filter:
                errors += 1

        error_rate = float(errors) / self.BLOOM_CAPACITY;

        self.assertLess(error_rate, self.BLOOM_ERROR_RATE*2, 
                   msg="Too many keys not included were found in the Bloom Filter (false positives), above the double expected error rate of "+str(self.BLOOM_ERROR_RATE))

    def tearDown(self):
        pass

if __name__ == "__main__": 
    unittest.main() 