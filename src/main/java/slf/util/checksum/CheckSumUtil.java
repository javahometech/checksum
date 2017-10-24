package slf.util.checksum;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CheckSumUtil {
	private static String SHA_256 = "SHA-256";
	private static String MD5 = "MD5";
	private static String CRC32 = "CRC32";

	public static void main(String args[]) throws Exception {
		if (args == null || args.length != 2) {
			System.out.println("Invalid Options provided");
			System.out.println("Valid options <file-path>  <SHA-256>");
			return;
		}
		boolean hdfsFileSystem = args[0].contains("hdfs://");
		InputStream is = null;
		if (hdfsFileSystem) {
			Configuration configuration = new Configuration();
			URI uri = new URI(args[0]);
			FileSystem hdfs = FileSystem.get(uri, configuration);
			Path path = new Path(uri.getPath());
			is = hdfs.open(path);
		}else {
			is = new FileInputStream(new File(args[0]));
			if (SHA_256.equalsIgnoreCase(args[1])) {
				System.out.println(getSHA256Checksum(is));;
			} else if (MD5.equalsIgnoreCase(args[1])) {
				System.out.println(getMD5Checksum(is));;
			} else if (CRC32.equalsIgnoreCase(args[1])) {
				System.out.println(getCRC32Checksum(is));
			}
		}
		

	}

	private static String getSHA256Checksum(InputStream fis) throws NoSuchAlgorithmException, Exception {
		return getFileChecksum(fis, MessageDigest.getInstance(SHA_256));
	}

	private static String getMD5Checksum(InputStream fis) throws NoSuchAlgorithmException, Exception {
		return getFileChecksum(fis, MessageDigest.getInstance(MD5));
	}

	private static String getFileChecksum(InputStream fis, MessageDigest digest) throws Exception {

		// Get file input stream for reading the file content
		try {
			// Create byte array to read data in chunks
			byte[] byteArray = new byte[1024];
			int bytesCount = 0;

			// Read file data and update in message digest
			while ((bytesCount = fis.read(byteArray)) != -1) {
				digest.update(byteArray, 0, bytesCount);
			}
			// Get the hash's bytes
			byte[] bytes = digest.digest();

			// This bytes[] has bytes in decimal format;
			// Convert it to hexadecimal format
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < bytes.length; i++) {
				sb.append(Integer.toString((bytes[i] & 0xff) + 0x100, 16).substring(1));
			}

			// return complete hash
			return sb.toString();
		} catch (Exception e) {
			e.printStackTrace();
			return "";
		} finally {
			try {
				if (fis != null)
					fis.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	private static long getCRC32Checksum(InputStream in) throws Exception {
		try {
			Checksum sum_control = new CRC32();
			for (int b = in.read(); b != -1; b = in.read()) {
				sum_control.update(b);
			}
			return sum_control.getValue();
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			in.close();
		}
	}
}