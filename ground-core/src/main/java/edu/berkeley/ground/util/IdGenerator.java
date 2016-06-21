package edu.berkeley.ground.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

public class IdGenerator {
    private static int counter = 0;

    private static int numBytes = 20;
    private static final String SEED = byteArrayToString(new SecureRandom().generateSeed(numBytes));

    public static String generateId(String baseId) {

        return SHA1(SEED + counter++ + baseId);
    }

    private static String SHA1(String str) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA1");
            return byteArrayToString(md.digest(str.getBytes()));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("FATAL ERROR: No SHA1 algorithm found in MessageDigest.");
        }
    }

    private static String byteArrayToString(byte[] array) {
        StringBuilder sb = new StringBuilder();

        for (byte b : array) {
            sb.append(Integer.toString(b & 0xff + 0x100, 16).substring(1));
        }

        return sb.toString();
    }
}
