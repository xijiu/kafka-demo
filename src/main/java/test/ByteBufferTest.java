package test;

import java.nio.ByteBuffer;

public class ByteBufferTest {
//    private static int length = 100 * 1024 * 1024;
    private static int length = 800 ;

    private static byte[] testByteArr = new byte[1024 * 1024];

    public static void main(String[] args) {
        ByteBufferTest byteBufferTest = new ByteBufferTest();
//        byteBufferTest.testArray();
        byteBufferTest.testDirectByteBuffer();
//        byteBufferTest.testHeapByteBuffer();
    }

    private void testHeapByteBuffer() {
        long begin1 = System.currentTimeMillis();
        ByteBuffer byteBuffer = ByteBuffer.allocate(800 * 1024 * 1024);
        System.out.println("allocate cost heap: " + (System.currentTimeMillis() - begin1));
        long begin = System.currentTimeMillis();
        for (int i = 0; i < length; i++) {
            byteBuffer.put(testByteArr);
//            byteBuffer.putLong((long) i);
        }
        System.out.println("heap byte buffer time cost : " + (System.currentTimeMillis() - begin));
    }

    private void testDirectByteBuffer() {
        long begin1 = System.currentTimeMillis();
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(800 * 1024 * 1024);
        System.out.println("allocate cost direct: " + (System.currentTimeMillis() - begin1));
        long begin = System.currentTimeMillis();
        for (int i = 0; i < length; i++) {
            byteBuffer.put(testByteArr);
//            byteBuffer.putLong((long) i);
        }
        System.out.println("direct byte array time cost : " + (System.currentTimeMillis() - begin));
    }

    private void testArray() {
        long[] bytes = new long[length];
        long begin = System.currentTimeMillis();
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (long) i;
        }
        System.out.println("byte array time cost : " + (System.currentTimeMillis() - begin));
    }

}
