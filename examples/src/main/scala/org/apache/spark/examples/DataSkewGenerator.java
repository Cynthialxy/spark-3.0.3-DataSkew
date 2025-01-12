package org.apache.spark.examples;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class DataSkewGenerator {
    public static void main(String[] args) {
        String[] targetIps = {
                "106.38.176.185", // 高频IP
                "106.38.176.117",
                "106.38.176.118",
                "106.38.176.116"
        };

        Random random = new Random();

        int BYTES_PER_LINE = 33; // 假设每行大约33字节
        long TARGET_SIZE_MB = 5;
        long BYTES_PER_GB = 1024 * 1024 * 1024;
        long TOTAL_LINES = (TARGET_SIZE_MB * BYTES_PER_GB) / BYTES_PER_LINE;

        int highFrequency = 100; // 高频IP相对比例
        int lowFrequency = 1;      // 其他IP的比例

        long targetRecords = (long)(TOTAL_LINES * highFrequency / (highFrequency + lowFrequency));
        long otherRecords = (long)(TOTAL_LINES * lowFrequency / (highFrequency + lowFrequency));

        // 生成数据文件
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("D://data_skew5gb.txt"))) {
            // 高频率的target_ip
            for (long i = 0; i < targetRecords; i++) {
                String clientIp = String.format("192.168.%d.%d", random.nextInt(256), random.nextInt(256));
                writer.write("106.38.176.185," + clientIp);
                writer.newLine();
            }

            // 低频率的target_ip
            for (long i = 0; i < otherRecords / 3; i++) {
                String clientIp = String.format("192.168.%d.%d", random.nextInt(256), random.nextInt(256));
                writer.write("106.38.176.117," + clientIp);
                writer.newLine();
            }
            for (long i = 0; i < otherRecords / 3; i++) {
                String clientIp = String.format("192.168.%d.%d", random.nextInt(256), random.nextInt(256));
                writer.write("106.38.176.118," + clientIp);
                writer.newLine();
            }
            for (long i = 0; i < otherRecords / 3; i++) {
                String clientIp = String.format("192.168.%d.%d", random.nextInt(256), random.nextInt(256));
                writer.write("106.38.176.116," + clientIp);
                writer.newLine();
            }
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Data generation completed. Total records: " + TARGET_SIZE_MB+"GB");
    }
}