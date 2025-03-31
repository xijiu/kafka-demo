package kafka.XW_DDS;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public class SocketTest {


    public static void main(String[] args) throws Exception {
        long begin = System.currentTimeMillis();
        startClient();
        System.out.println("total time cost : " + (System.currentTimeMillis() - begin) + " ms");
    }

    public static void startClient() throws Exception {
        Selector selector = Selector.open();
        SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress("10.253.246.5", 9095));
        socketChannel.configureBlocking(false);
        SelectionKey selectionKey = socketChannel.register(selector, SelectionKey.OP_READ);
        selectionKey.attach(null);

        SocketChannel socketChannel2 = (SocketChannel) selectionKey.channel();
        Socket socket = socketChannel2.socket();

        long begin = System.currentTimeMillis();
        String hostName = socket.getInetAddress().getHostName();
        System.out.println("host name is " + hostName + ", and cost : " + (System.currentTimeMillis() - begin) + " ms");

        long begin2 = System.currentTimeMillis();
        String hostAddress = socket.getInetAddress().getHostAddress();
        System.out.println("host address is " + hostAddress + ", and cost : " + (System.currentTimeMillis() - begin2) + " ms");

    }
}
