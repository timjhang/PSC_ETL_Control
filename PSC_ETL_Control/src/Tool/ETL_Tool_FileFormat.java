package Tool;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class ETL_Tool_FileFormat {
	
	public static boolean checkBytesList(List<byte[]> list) {
		
		// 基本檢核  list為null回傳false
		if (list == null) {
			System.out.println("ETL_Tool_FileFormat -- list 為  null");
			return false;
		}
		
		// 基本檢核  list的size為0回傳false
		if (list.size() == 0) {
			System.out.println("ETL_Tool_FileFormat -- list size = 0");
			return false;
		}
		
		boolean result = true;
		
		// list size最少應有2
		if (list.size() < 2) {
			System.out.println("ETL_Tool_FileFormat -- list size = " + list.size());
			result = false;
		}
		
		try {
			// 第一筆應為首錄1
			String firstWord = getFistWord(list.get(0));
			if (!"1".equals(firstWord)) {
				System.out.println("ETL_Tool_FileFormat -- 第一筆代碼錯誤:" + firstWord + ",不為1！");
				result = false;
			}
			
			// 最後一筆應為尾錄3
			firstWord = getFistWord(list.get(list.size() - 1));
			if (!"3".equals(firstWord)) {
				System.out.println("ETL_Tool_FileFormat -- 最後一筆代碼錯誤:" + firstWord + ",不為3！");
				result = false;
			}
			
			// 首尾中間為明細，應為2
			if (list.size() > 2) {
				for (int i = 1; i < list.size() - 1; i++) {
					firstWord = getFistWord(list.get(i));
					if (!"2".equals(firstWord)) {
						System.out.println("ETL_Tool_FileFormat -- 中間第" + (i + 1) + "筆代碼錯誤:" + firstWord + ",不為2！");
						result = false;
					}
				}
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
			result = false;
		}
		
		return result;
	}
	
	private static String getFistWord(byte[] byteArray) throws UnsupportedEncodingException {
		
		byte[] firstWord = new byte[1];
		firstWord[0] = byteArray[0];
//		System.out.println(new String(firstWord, "UTF-8"));  // for test
		return new String(firstWord, "UTF-8");
	}

}
