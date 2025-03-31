package test;

import java.nio.ByteBuffer;

public class ByteBufferTest2 {
//    private static int length = 100 * 1024 * 1024;
    private static int singleLength = 1;
    private static int totalSize = 1 * 1024 * 1024 * 1024 ;
    private static int times = (int) (totalSize / singleLength);

    private static byte[] testByteArr = new byte[singleLength];

    public static void main(String[] args) {
        ByteBufferTest2 byteBufferTest = new ByteBufferTest2();
        byteBufferTest.testHeapByteBuffer(true);
//        byteBufferTest.testHeapByteBuffer(false);
    }

    private void testHeapByteBuffer(boolean direct) {
        String str = direct ? "direct" : "heap";
        long begin1 = System.currentTimeMillis();
        ByteBuffer byteBuffer;
        if (direct) {
            byteBuffer = ByteBuffer.allocateDirect(totalSize);
        } else {
            byteBuffer = ByteBuffer.allocate(totalSize);
        }
        System.out.println(str + ", allocate cost heap: " + (System.currentTimeMillis() - begin1));


        long begin = System.currentTimeMillis();
        for (int i = 0; i < times; i++) {
//            byteBuffer.put(testByteArr);
            byteBuffer.put((byte) i);
        }
        System.out.println(str + ", byte buffer time cost : " + (System.currentTimeMillis() - begin));
    }

}
