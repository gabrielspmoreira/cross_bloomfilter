package cross_bloomfilter.bloomfilter.impl;

import java.util.zip.CRC32;

class BloomFilterCommons {
	/**
     * Computes the checksum of bytes
     * @param data Bytes to calculate checksum
     * @return A checksum number
     */
    static short computeChecksum(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data);
        long checksum32 = crc.getValue();
        return (short) ((checksum32 & 0xFFFF) ^ (checksum32 >> 16));
    }

}
