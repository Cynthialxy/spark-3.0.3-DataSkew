package org.apache.spark.examples;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;


/** 创造2g数据 */
public class CreateData {
    /** 保存路径 */
    private static final String FILE_PATH = "D:\\spark\\bigdata.txt";
    private static final int BYTES_PER_LINE = 100; // 假设每行大约100字节
    private static final long TARGET_SIZE_GB = 2;
    private static final long BYTES_PER_GB = 1024 * 1024 * 1024;
    private static final long TOTAL_LINES = (TARGET_SIZE_GB * BYTES_PER_GB) / BYTES_PER_LINE;

    public static void main(String[] args) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(FILE_PATH))) {
            for (long i = 0; i < TOTAL_LINES; i++) {
                String line = generateRandomLine(BYTES_PER_LINE);
                writer.write(line);
                if (i < TOTAL_LINES - 1) {
                    writer.newLine();
                }
            }
            System.out.println("Data generation complete.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String generateRandomLine(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            char randomChar = (char) ('a' + (Math.random() * 26)); // 生成一个随机的小写字母
            sb.append(randomChar);
        }
        return sb.toString();
    }
}
