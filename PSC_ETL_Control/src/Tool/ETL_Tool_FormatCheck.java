package Tool;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ETL_Tool_FormatCheck {
	// ETL 檢核工具
	
	/**
	 * 空值檢核工具  (空值:true\非空值:false)
	 * @param input
	 * @return	true 空值 / false 非空值
	 */
	public static boolean isEmpty(String input) {
		if (input == null || "".equals(input.trim())||input.replaceAll("[ |　]", " ").trim().length() == 0) {
			return true;
		}
		
		return false;
	}
	
	/**
	 * 檢測字串是否符合Timestamp格式 yyyyMMddHHmmss
	 * @param dateStr 檢測字串
	 * @return true 成功 / false 失敗
	 */
	public static boolean checkTimestamp(String dateStr) {
		if (isEmpty(dateStr)) {
			return false;
		}
		
		// 00000000000000代表1900/01/01 00:00:00
		if ("00000000000000".equals(dateStr)) {
			return true;
		}
		
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
			sdf.setLenient(false); // 過濾不合理日期
			sdf.parse(dateStr);
			return true;
		} catch (Exception ex) {
			//System.out.println(ex.getMessage());
			return false;
		}
	}
	
	// 日期格式檢核工具  (通過檢核:true\檢核失敗:false)
	public static boolean checkDate(String dateStr) {
		
		if (isEmpty(dateStr)) {
			return false;
		}
		
		// 00000000代表1900/01/01
		if ("00000000".equals(dateStr)) {
			return true;
		}
		
		
		if (dateStr.length()!=8) {
			return false;
		}
		
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			sdf.setLenient(false); // 過濾不合理日期
			Date realDate = sdf.parse(dateStr);
			return true;
		} catch (Exception ex) {
			//System.out.println(ex.getMessage());
			return false;
		}
	}
	

	// 日期格式檢核工具  (通過檢核:true\檢核失敗:false)  String pattern 格式 ex: "yyyy-MM-dd HH:mm:ss.SSSSSS"
	public static boolean checkDate(String inputString, String pattern)
	{ 
	    SimpleDateFormat format = new SimpleDateFormat(pattern);
	    try{
		   format.setLenient(false); // 過濾不合理日期
	       format.parse(inputString);
	       return true;
	    }catch(Exception e)
	    {
	        return false;
	    }
	}
	
	// 數字檢核工具  (通過檢核:true\檢核失敗:false)
	public static boolean checkNum(String numStr) {
		if (isEmpty(numStr)) {
			return false;
		}
		
		try {
			long realNum = Long.parseLong(numStr);
		} catch (Exception ex) {
			System.out.println(ex.getMessage());
			return false;
		}
		
		return true;
	}
	
	
	public static void main(String[] argv) throws Exception {
		System.out.println(ETL_Tool_FormatCheck.checkDate("20180399"));

	}

}
