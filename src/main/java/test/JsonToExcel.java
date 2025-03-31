package test;//package test;
//
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.apache.poi.ss.usermodel.*;
//import org.apache.poi.xssf.usermodel.XSSFWorkbook;
//
//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileNotFoundException;
//import java.io.FileOutputStream;
//import java.io.FileReader;
//import java.io.IOException;
//import java.util.Iterator;
//
//public class JsonToExcel {
//
//    public static void main(String[] args) throws Exception {
//        String file = "/Users/likangning/Downloads/topics.json";
//
//        String jsonString = readFileContent(file);
//
//        // 使用 Jackson 解析 JSON 字符串
//        ObjectMapper objectMapper = new ObjectMapper();
//        JsonNode rootNode = objectMapper.readTree(jsonString);
//        JsonNode topicsNode = rootNode.path("topics");
//
//        // 2. 创建 Excel 工作簿
//        Workbook workbook = new XSSFWorkbook();
//        Sheet sheet = workbook.createSheet("Kafka Topics");
//
//        // 3. 创建 Excel 表头
//        Row headerRow = sheet.createRow(0);
//        headerRow.createCell(0).setCellValue("Topic");
//        headerRow.createCell(1).setCellValue("Partition Number");
//        headerRow.createCell(2).setCellValue("Replication Factor");
//
//        // 4. 遍历 topics 数据，填充到 Excel 表格中
//        int rowNum = 1;
//        Iterator<JsonNode> topicsIterator = topicsNode.iterator();
//        while (topicsIterator.hasNext()) {
//            JsonNode topicNode = topicsIterator.next();
//
//            // 获取每个 topic 的配置信息
//            String topic = topicNode.path("topic").asText();
//            int partitions = topicNode.path("partitions").asInt();
//            int replicationFactor = topicNode.path("replicationFactor").asInt();
//
//            // 创建一行数据并填充
//            Row row = sheet.createRow(rowNum++);
//            row.createCell(0).setCellValue(topic);
//            row.createCell(1).setCellValue(partitions);
//            row.createCell(2).setCellValue(replicationFactor);
//        }
//
//        // 5. 写入 Excel 文件
//        File outputFile = new File("KafkaTopicsConfig.xlsx");
//        if (outputFile.exists()) {
//            outputFile.delete();
//        }
//        try (FileOutputStream fileOut = new FileOutputStream(outputFile)) {
//            workbook.write(fileOut);
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            workbook.close();
//        }
//
//        System.out.println("Excel file created");
//    }
//
//    private static String readFileContent(String file) throws IOException {
//        try (BufferedReader br = new BufferedReader(new FileReader(file));) {
//            StringBuilder sb = new StringBuilder();
//            String line;
//            while ((line = br.readLine()) != null) {
//                sb.append(line);
//            }
//            return sb.toString();
//        }
//    }
//}
