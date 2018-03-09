package Tool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class ETL_Tool_FileByteUtil {
	
	public static List<byte[]> getFilesBytes(String path) throws IOException {
		List<byte[]> list = new ArrayList<byte[]>();

		byte[] bytes = Files.readAllBytes(Paths.get(path));

		StringBuffer hexStr = new StringBuffer(bytes.length * 2);

		for (byte b : bytes) {
			hexStr.append(String.format("%02X", b));

		}
		String[] linesHexStr = hexStr.toString().split("0D0A");

		for (String s : linesHexStr) {
			list.add(hexStrToByteArray(s));
		}
		return list;
	}

	private static byte[] hexStrToByteArray(String str) {
		if (str == null) {
			return null;
		}
		if (str.length() == 0) {
			return new byte[0];
		}
		byte[] byteArray = new byte[str.length() / 2];
		for (int i = 0; i < byteArray.length; i++) {
			String subStr = str.substring(2 * i, 2 * i + 2);
			byteArray[i] = ((byte) Integer.parseInt(subStr, 16));
		}
		return byteArray;
	}

	public void main(String[] args) throws Exception {
		String path = "C:\\Users\\Ian\\Desktop\\018\\018_CALENDAR_20180116.TXT";

		long time1, time2;
		time1 = System.currentTimeMillis();
		getFilesBytes(path);
		time2 = System.currentTimeMillis();
		System.out.println("花了：" + (time2 - time1) + "豪秒");
	}
}