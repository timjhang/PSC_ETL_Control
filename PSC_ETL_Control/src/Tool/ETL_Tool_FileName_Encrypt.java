package Tool;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ETL_Tool_FileName_Encrypt {

	private static final char[] hexChar = {
	    '0', '1', '2', '3', '4', '5', '6', '7',
	    '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
    };
	    
    private static byte[] eccrypt(String info) throws NoSuchAlgorithmException {

		MessageDigest md5 = MessageDigest.getInstance("MD5");
		byte[] srcBytes = info.getBytes();

		md5.update(srcBytes);

		byte[] resultBytes = md5.digest();
		return resultBytes;
	}

	public static String encode(String msg) throws NoSuchAlgorithmException {
		String result = null;
		ETL_Tool_FileName_Encrypt md5 = new ETL_Tool_FileName_Encrypt();
		byte[] resultBytes = md5.eccrypt(msg);
		result = toHexString(resultBytes);
		if (result.length()>20) {
			result = result.substring(8, 18);
		}
		
		return result;
	}

	private static String toHexString(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (int i = 0; i < b.length; i++) {
            sb.append(hexChar[(b[i] & 0xf0) >>> 4]);
            sb.append(hexChar[b[i] & 0x0f]);
        }
        return sb.toString();
    }
		
	public static void main(String[] args) {
		//AML_951_20180116_001.zip
		try {
			System.out.println(ETL_Tool_FileName_Encrypt.encode("AML_018_20180409001.zip"));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
