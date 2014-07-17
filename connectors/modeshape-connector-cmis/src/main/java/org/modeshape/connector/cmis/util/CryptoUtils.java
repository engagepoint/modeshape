package org.modeshape.connector.cmis.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Scanner;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * crypt/decrypt util.
 */
public class CryptoUtils {

    /**
     * SLF logger.
     */
    public static final Logger LOG = LoggerFactory.getLogger(CryptoUtils.class);
    /**
     * algorithm.
     */
    private static final String AES = "AES";
    /**
     * hex key.
     */
    public static final String KEY_AS_HEX = "4A92FD95BE77E1729FD0622A034613C7";

    /**
     * main method.
     * @param args password
     * @throws Exception exc
     */
    public static void main(final String[] args) throws Exception {
        for (String filePath : args) {
            LOG.info("Trying read file - " + filePath);
            Properties prop = new Properties();
            prop.load(new FileReader(filePath));
            // encode all
            for (Entry<Object, Object> pair : prop.entrySet()) {
                pair.setValue(CryptoUtils.encrypt((String) pair.getValue(), KEY_AS_HEX));
            }
            // save all
            prop.store(new FileWriter(filePath), "");
        }
        //test();
    }

    /**
     * encrypt a value and generate a keyfile if the keyfile is not found then a
     * new one is created.
     *
     * @throws java.security.GeneralSecurityException
     *
     * @throws java.io.IOException
     */
    public static String encrypt(final String value, final File keyFile)
            throws GeneralSecurityException, IOException {
        if (!keyFile.exists()) {
            KeyGenerator keyGen = KeyGenerator.getInstance(CryptoUtils.AES);
            keyGen.init(128);
            SecretKey sk = keyGen.generateKey();
            FileWriter fw = new FileWriter(keyFile);
            fw.write(byteArrayToHexString(sk.getEncoded()));
            fw.flush();
            fw.close();
        }

        SecretKeySpec sks = getSecretKeySpec(keyFile);
        Cipher cipher = Cipher.getInstance(CryptoUtils.AES);
        cipher.init(Cipher.ENCRYPT_MODE, sks, cipher.getParameters());
        byte[] encrypted = cipher.doFinal(value.getBytes());
        return byteArrayToHexString(encrypted);
    }

    /**
     * encrypt a value.
     *
     * @throws java.security.GeneralSecurityException
     *
     * @throws java.io.IOException
     */
    public static String encrypt(String value, String keyAsHex)
            throws GeneralSecurityException, IOException {
        SecretKeySpec sks = getSecretKeySpec(keyAsHex);
        Cipher cipher = Cipher.getInstance(CryptoUtils.AES);
        cipher.init(Cipher.ENCRYPT_MODE, sks, cipher.getParameters());
        byte[] encrypted = cipher.doFinal(value.getBytes());
        return byteArrayToHexString(encrypted);
    }

    /**
     * decrypt a value.
     *
     * @throws java.security.GeneralSecurityException
     *
     * @throws java.io.IOException
     */
    public static String decrypt(String message, File keyFile)
            throws GeneralSecurityException, IOException {
        SecretKeySpec sks = getSecretKeySpec(keyFile);
        Cipher cipher = Cipher.getInstance(CryptoUtils.AES);
        cipher.init(Cipher.DECRYPT_MODE, sks);
        byte[] decrypted = cipher.doFinal(hexStringToByteArray(message));
        return new String(decrypted);
    }

    /**
     * decrypt a value.
     *
     * @throws java.security.GeneralSecurityException
     *
     * @throws java.io.IOException
     */
    public static String decrypt(String message, String keyAsHex)
            throws GeneralSecurityException, IOException {
        SecretKeySpec sks = getSecretKeySpec(keyAsHex);
        Cipher cipher = Cipher.getInstance(CryptoUtils.AES);
        cipher.init(Cipher.DECRYPT_MODE, sks);
        byte[] decrypted = cipher.doFinal(hexStringToByteArray(message));
        return new String(decrypted);
    }

    private static SecretKeySpec getSecretKeySpec(File keyFile)
            throws NoSuchAlgorithmException, IOException {
        byte[] key = readKeyFile(keyFile);
        SecretKeySpec sks = new SecretKeySpec(key, CryptoUtils.AES);
        return sks;
    }

    private static SecretKeySpec getSecretKeySpec(String keyAsHex)
            throws NoSuchAlgorithmException, IOException {
        byte[] key = hexStringToByteArray(keyAsHex);
        SecretKeySpec sks = new SecretKeySpec(key, CryptoUtils.AES);
        return sks;
    }

    private static byte[] readKeyFile(File keyFile)
            throws FileNotFoundException {
        Scanner scanner = new Scanner(keyFile).useDelimiter("\\Z");
        String keyValue = scanner.next();
        scanner.close();
        return hexStringToByteArray(keyValue);
    }

    private static String byteArrayToHexString(byte[] b) {
        StringBuffer sb = new StringBuffer(b.length * 2);
        for (int i = 0; i < b.length; i++) {
            int v = b[i] & 0xff;
            if (v < 16) {
                sb.append('0');
            }
            sb.append(Integer.toHexString(v));
        }
        return sb.toString().toUpperCase();
    }

    private static byte[] hexStringToByteArray(String s) {
        byte[] b = new byte[s.length() / 2];
        for (int i = 0; i < b.length; i++) {
            int index = i * 2;
            int v = Integer.parseInt(s.substring(index, index + 2), 16);
            b[i] = (byte) v;
        }
        return b;
    }
}