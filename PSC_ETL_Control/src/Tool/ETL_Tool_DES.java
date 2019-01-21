package Tool;

import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;

public class ETL_Tool_DES {

	private static final String KEY = "6tfcnhy6";

	private static byte[] encryptBytes(String content) {
		try {
			SecureRandom random = new SecureRandom();
			DESKeySpec desKey = new DESKeySpec(KEY.getBytes());
			SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
			SecretKey securekey = keyFactory.generateSecret(desKey);
			Cipher cipher = Cipher.getInstance("DES");
			cipher.init(Cipher.ENCRYPT_MODE, securekey, random);
			byte[] result = cipher.doFinal(content.getBytes());
			return result;
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String decrypt(String hexContent) {
		try {
			byte[] content = hexStr2ByteArr(hexContent);
			SecureRandom random = new SecureRandom();
			DESKeySpec desKey = new DESKeySpec(KEY.getBytes());
			SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
			SecretKey securekey = keyFactory.generateSecret(desKey);
			Cipher cipher = Cipher.getInstance("DES");
			cipher.init(Cipher.DECRYPT_MODE, securekey, random);
			byte[] result = cipher.doFinal(content);
			return new String(result);
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return null;
	}

	private static String byteArr2HexStr(byte[] arrB) throws Exception {
		int iLen = arrB.length;
		// 每個byte用兩個字符才能表示，所以字符串的長度是數組長度的兩倍
		StringBuffer sb = new StringBuffer(iLen * 2);
		for (int i = 0; i < iLen; i++) {
			int intTmp = arrB[i];
			// 把負數轉換為正數
			while (intTmp < 0) {
				intTmp = intTmp + 256;
			}
			// 小於0F的數需要在前補0
			if (intTmp < 16) {
				sb.append("0");
			}
			sb.append(Integer.toString(intTmp, 16));
		}
		return sb.toString();
	}

	private static byte[] hexStr2ByteArr(String strIn) throws Exception {
		byte[] arrB = strIn.getBytes();
		int iLen = arrB.length;

		// 兩個字符表示一個字節，所以字節數組長度是字符串長度除以2
		byte[] arrOut = new byte[iLen / 2];
		for (int i = 0; i < iLen; i = i + 2) {
			String strTmp = new String(arrB, i, 2);
			arrOut[i / 2] = (byte) Integer.parseInt(strTmp, 16);
		}
		return arrOut;
	}
	
	private static String encrypt(String input) throws Exception {
		byte[] result = encryptBytes(input);
		
		return byteArr2HexStr(result);
	}

	public static void main(String[] args) {
//		byte[] result = encryptBytes("");
		try {
//			System.out.println(byteArr2HexStr(result));
//			System.out.println(decrypt(""));
			
//			System.out.println(encrypt("172.16.16.141"));
//			System.out.println(encrypt("25"));
//			System.out.println(encrypt("SRC"));
//			System.out.println(encrypt("SRC"));
//			System.out.println(encrypt("jdbc:db2://172.18.6.133:50000/GAML"));
//			System.out.println(encrypt(":currentschema=SRC;currentFunctionPath=SRC;"));
//			System.out.println(ETL_Tool_DES.decrypt("58fcd8a8d6fa0357b43596ad4e09a6c4493ff949a"
//					+ "610bfc777432c83a5696d7645fb4928d65fcffb14ecc533031ffd"
//					+ "1ea810009434c9f055571abf832b087a8c949513758b0ed27dd81b3b4aede62aba"));
//			System.out.println(ETL_Tool_DES.decrypt("210854254ff94eec32f326325a431d9a248f310"
//					+ "40be3a7193b4f37cefd0fe16ddafed100689a1ac85961605e87949653"));
			
//			System.out.println(encrypt("jdbc:db2://172.18.6.151:50000/ETLDB"));
//			System.out.println(encrypt("jdbc:db2://172.18.6.152:50000/ETLDB"));
//			System.out.println(encrypt(":currentschema="));
//			System.out.println(encrypt(";currentFunctionPath="));
//			System.out.println(encrypt(";"));
			
			System.out.println(ETL_Tool_DES.decrypt("4ac3ce9609e110fee69d06fd1ec8c823"));
			System.out.println(ETL_Tool_DES.decrypt("f8359cb46aebc26d"));
//			System.out.println(ETL_Tool_DES.decrypt("29ee04b861a87da1"));
			
//			System.out.println(encrypt("40"));
//			System.out.println(encrypt("12"));
//			System.out.println(encrypt("C:\\ETL\\DM"));
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
