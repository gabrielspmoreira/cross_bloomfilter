#!/usr/bin/python
# -- coding: utf-8 --

import bitarray
import math
import mmh3
import binascii
import base64
import gzip
from io import BytesIO

def zip_bytes(data):
    fgz = BytesIO()
    with gzip.GzipFile(mode='wb', fileobj=fgz) as gzip_obj:
        gzip_obj.write(data)
    return fgz.getvalue()

def unzip_bytes(zipped_data):
    fgz = BytesIO(zipped_data)
    with gzip.GzipFile(mode='rb', fileobj=fgz) as gzip_obj:
        return gzip_obj.read()

class BloomFilter(object):

    def __init__(self, capacity=100000, error=0.005, endian='little', data=None):
        """
        Initialize a bloom filter with given capacity and false positive rate
        """
        self.capacity = capacity
        self.error = error
        self.num_bits = int(-capacity * math.log(error) / math.log(2) ** 2) + 1
        self._calc_num_hashes()
        self.data = data
        if not self.data:
            self.data = bitarray.bitarray(self.num_bits, endian=endian)        
            self.data.setall(0)

    def _calc_num_hashes(self):
        self.num_hashes = int(
            self.num_bits * math.log(2) / float(self.capacity)) + 1

    def _indexes(self, key):
        h1 = mmh3.hash(key, 42)
        h2 = mmh3.hash(key, h1)
        for i in range(self.num_hashes):
            yield BloomFilter._unsigned(int(h1 + i * h2)) % self.num_bits

    def add(self, key):
        for index in self._indexes(key):
            self.data[index] = True

    def extend(self, keys):
        for key in keys:
            self.add(key)

    def __contains__(self, key):
        return all(self.data[index] for index in self._indexes(key))

    def __len__(self):
        num_bits_on = self.data.count(True)
        return int(-1.0 * self.num_bits * \
            math.log(1.0 - num_bits_on / float(self.num_bits)) / \
            float(self.num_hashes))
    
    def dump(self, gzipped=True):
        data_bytes = self.data.tobytes()
        
        if gzipped:
            data_bytes = zip_bytes(data_bytes)

        checksum = BloomFilter._compute_checksum(data_bytes)
        inverted_error = int(1.0 / self.error)
        capacity = self.capacity     

        checksum_bytes = BloomFilter._int_to_bytes(checksum, 2)
        gzipped_bytes = BloomFilter._int_to_bytes(int(gzipped), 1)
        inverted_error_bytes = BloomFilter._int_to_bytes(inverted_error, 2)
        capacity_bytes = BloomFilter._int_to_bytes(capacity, 4)

        #Adding 9 bytes header
        dumped_bytes = checksum_bytes
        dumped_bytes += gzipped_bytes
        dumped_bytes += inverted_error_bytes
        dumped_bytes += capacity_bytes

        #Adding bitarray data
        dumped_bytes += data_bytes

        return dumped_bytes

    def dump_to_base64_str(self, gzipped=True):
        data_bytes = self.dump(gzipped=gzipped)
        encoded_dump_str = base64.urlsafe_b64encode(data_bytes).decode('ascii')
        return encoded_dump_str

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__dict__ == other.__dict__
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    @staticmethod
    def load(dump_bytes, endian='little'):
        #Parsing 8 bytes header
        checksum = int.from_bytes(dump_bytes[0:2], byteorder=endian)
        gzipped = bool(int.from_bytes(dump_bytes[2:3], byteorder=endian))
        inverted_error = int.from_bytes(dump_bytes[3:5], byteorder=endian)        
        capacity = int.from_bytes(dump_bytes[5:9], byteorder=endian)
        #Parsing bitarray data
        bitarray_data = dump_bytes[9:]
        
        if checksum != BloomFilter._compute_checksum(bitarray_data):
            raise Exception("Bad checksum")
            
        if gzipped:
            bitarray_data = unzip_bytes(bitarray_data)

        error = 1.0 / inverted_error

        new_bitarray = bitarray.bitarray(endian=endian)
        new_bitarray.frombytes(bitarray_data)
        num_bits = new_bitarray.length()

        return BloomFilter(data=new_bitarray, capacity=capacity, error=error, endian=endian)

    @staticmethod
    def load_from_base64_str(dump_bytes, endian='little'):
        bloom_decoded_bytes = dump_bytes.encode('ascii')
        bloom_decoded = base64.urlsafe_b64decode(bloom_decoded_bytes)
        return BloomFilter.load(bloom_decoded, endian=endian)

    @staticmethod
    def _unsigned(number):
        return number & 0xffffffff

    @staticmethod
    def _compute_checksum(thebytes):
        checksum32 = binascii.crc32(thebytes)
        checksum = (checksum32 & 0xFFFF) ^ (checksum32 >> 16)
        return int(checksum)

    @staticmethod
    def _int_to_bytes(number, length, endian='little'):
        return (number).to_bytes(length, byteorder=endian)

    @staticmethod
    def union(bloom_a, bloom_b):
        assert bloom_a.capacity == bloom_b.capacity, "Capacities must be equal"
        assert bloom_a.error == bloom_b.error, "Error rates must be equal"

        bloom_union = BloomFilter(bloom_a.capacity, bloom_a.error)
        bloom_union.data = bloom_a.data | bloom_b.data
        return bloom_union