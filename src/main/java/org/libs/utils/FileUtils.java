package org.libs.utils;

import java.io.*;

public class FileUtils {

    public static String readHexFromTxtFile(String path) throws IOException {
        StringBuilder hexBuilder = new StringBuilder();

//        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
//            String line;
//            while ((line = reader.readLine()) != null) {
//                hexBuilder.append(line.trim());
//            }
//        }
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(path), "UTF-8"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                hexBuilder.append(line.trim());
            }
        }

        return hexBuilder.toString();
    }
    public static byte[] hexStringToByteArray(String hex) {
        int len = hex.length();
        if (len % 2 != 0) {
            throw new IllegalArgumentException("Hex string must have even length");
        }

        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) Integer.parseInt(hex.substring(i, i + 2), 16);
        }
        return data;
    }
}
