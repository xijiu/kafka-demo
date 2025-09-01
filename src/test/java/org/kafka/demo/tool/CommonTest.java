package org.kafka.demo.tool;

import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Set;
import java.util.TreeSet;

public class CommonTest {


    @Test
    public void test() throws Exception {
        // 创建 HttpClient 实例
        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        // 构建 GET 请求
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:9999/metrics"))
                .header("Content-Type", "application/json")
                .GET()
                .build();

        // 发送同步请求并获取响应
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        // 处理响应
        System.out.println("Status code: " + response.statusCode());
        String body = response.body();
        String[] split = body.split("\n");
        Set<String> set = new TreeSet<>();
        for (String line : split) {
            if (line.startsWith("# TYPE")) {
                set.add(line);
            }
        }
        System.out.println("size is " + set.size());
        for (String ele : set) {
            String[] split1 = ele.split(" ");
            System.out.println(split1[2]);
        }
    }
}
