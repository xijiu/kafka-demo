package org.kafka.demo;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.junit.Test;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

public class ProtocolFetchTest {

    private static final short version = 12;

    @Test
    public void fetchRequestTest() {

//        String hexString =
//                "0e 1f 48 2d 7e 32 06 82 25 e9 a3 d9 08 00 45 00 " +
//                        "00 ab 3f b3 40 00 40 06 35 ed 62 02 00 55 62 02 " +
//                        "00 54 eb 2e 23 85 68 57 32 37 b5 08 3f b2 80 18 " +
//                        "7d 2c c5 4a 00 00 01 01 08 0a b7 d1 e6 5c 26 30 " +
//                        "b9 11 00 00 00 73 00 01 00 0c 11 85 6f bf 00 15 " +
//                        "62 72 6f 6b 65 72 2d 31 30 30 32 2d 66 65 74 63 " +
//                        "68 65 72 2d 30 00 00 00 03 ea 00 00 01 f4 00 00 " +
//                        "00 01 00 a0 00 00 00 72 52 11 e0 11 85 6f bf 02 " +
//                        "13 5f 5f 63 6f 6e 73 75 6d 65 72 5f 6f 66 66 73 " +
//                        "65 74 73 02 00 00 00 15 00 00 00 03 00 00 00 00 " +
//                        "13 36 67 3a 00 00 00 03 00 00 00 00 00 00 00 00 " +
//                        "00 10 00 00 00 00 01 01 00";

        String hexString =
                "02f1de639d310e073d497b60080045000085699e40004006dfe1c0a800e12c8a02e0c3fa2387d907f1fe4e0e7bab80187d2cf16a00000101080a4f4582a618123be50000004d0001000c000af48c0025636f6e73756d65722d434d432d4343532d546573744d616e6167656d656e747277727a2d3400ffffffff000001f40000000103200000003fcc798c0008080101010100";

        StringBuilder sb = new StringBuilder();
        char[] charArray = hexString.toCharArray();
        for (char c : charArray) {
            if (c != ' ') {
                sb.append(c);
            }
        }
        hexString = sb.toString();
        byte[] byteArray = hexStringToByteArray(hexString);
        ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);
        System.out.println("len is : " + byteBuffer.array().length);
        byteBuffer.get(new byte[66]);
        byteBuffer.getInt();

        RequestHeader header = RequestHeader.parse(byteBuffer);
        System.out.println(header);
        System.out.println("byteBuffer remaining : " + byteBuffer.remaining());
        FetchRequest fetchRequest = FetchRequest.parse(byteBuffer, version);
        System.out.println(fetchRequest);
    }

    public static byte[] hexStringToByteArray(String hex) {
        // 确保字符串长度为偶数
        if (hex.length() % 2 != 0) {
            hex = "0" + hex;
        }
        return new BigInteger(hex, 16).toByteArray();
    }

    @Test
    public void fetchResponseTest() {

        String hexString =
                "" +
                        "0e073d497b6002f1de639d31080045000049ebd34000370666e82c8a02e0c0a800e12387c3fa4e0e7b6cd907f15c8018c526dbb100000101080a181237f94f457cc400000011000af488000000000000003fcc798c0100";

        StringBuilder sb = new StringBuilder();
        char[] charArray = hexString.toCharArray();
        for (char c : charArray) {
            if (c != ' ') {
                sb.append(c);
            }
        }
        hexString = sb.toString();
        byte[] byteArray = hexStringToByteArray(hexString);
        ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);
        System.out.println("len is : " + byteBuffer.array().length);
        byteBuffer.get(new byte[66]);
        byteBuffer.getInt();


        ResponseHeader responseHeader = ResponseHeader.parse(byteBuffer, (short) 0);
        System.out.println("responseHeader is " + responseHeader);
        System.out.println("byteBuffer remaining : " + byteBuffer.remaining());
        FetchResponse<MemoryRecords> fetchResponse = FetchResponse.parse(byteBuffer, version);
        System.out.println(fetchResponse);
        LinkedHashMap<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> map = fetchResponse.responseData();
        System.out.println("map size is : " + map.size());
        System.out.println("map is : " + map);
        for (Map.Entry<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> entry : map.entrySet()) {
            System.out.println();
            System.out.println();
            System.out.println();
            System.out.println();
            System.out.println("TP is : " + entry.getKey());
            FetchResponse.PartitionData<MemoryRecords> value = entry.getValue();
            MemoryRecords records = value.records();
            records.batches().forEach(batch -> {
                System.out.println("isValid: " + batch.isValid());
                System.out.println("crc : " + batch.checksum());
                System.out.println("baseOffset : " + batch.baseOffset());
            });
        }
    }
}