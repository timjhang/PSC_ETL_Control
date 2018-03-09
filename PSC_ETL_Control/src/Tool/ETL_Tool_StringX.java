package Tool;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ETL_Tool_StringX {

	/**
	 * 轉換字串為Timestamp型態，格式預設為yyyyMMddHHmmss
	 * @param dateStr 要轉換的字串
	 * @return 轉換後的Timestamp物件，如轉換失敗，則回傳null
	 */
	public static Timestamp toTimestamp(String dateStr) {
		Timestamp timestamp = null;
		Date parsedDate;
		
		// 00000000000000代表1900/01/01 00:00:00
		if ("00000000000000".equals(dateStr)) {
			dateStr = "19000101000000";
		}
		
		if(ETL_Tool_FormatCheck.checkTimestamp(dateStr)){
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
			try {
				parsedDate = dateFormat.parse(dateStr);
				timestamp = new Timestamp(parsedDate.getTime());
			} catch (ParseException e) {
				return timestamp;
			}
		}

		return timestamp;
	}
	

	/**
	 * 轉換字串為Timestamp型態，格式預設為yyyyMMddhhmmss
	 * @param dateStr 要轉換的字串
	 * @param pattern 轉換前的格式 ex:yyyyMMddhhmmss
	 * @return 轉換後的Timestamp物件，如轉換失敗，則回傳null
	 */
	public static Timestamp toTimestamp(String dateStr ,String pattern) {
		Timestamp timestamp = null;
		Date parsedDate;
		
		if( ETL_Tool_FormatCheck.checkDate(dateStr,pattern) ){
			SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
			try {
				parsedDate = dateFormat.parse(dateStr);
				timestamp = new Timestamp(parsedDate.getTime());
			} catch (ParseException e) {
				return timestamp;
			}
		}

		return timestamp;
	}
	
	/**
	 * 字串轉換為Util Date，格式預設為yyyyMMdd
	 * @param dateStr 轉換字串
	 * @return Util Date，有錯誤則回傳null
	 * @throws Exception
	 */
	public static Date toUtilDate(String dateStr)throws Exception {
		
		// 00000000代表1900/01/01
		if ("00000000".equals(dateStr)) {
			return new SimpleDateFormat("yyyyMMdd").parse("19000101");
		}
		
		return ETL_Tool_FormatCheck.checkDate(dateStr) ? new SimpleDateFormat("yyyyMMdd").parse(dateStr) : null;
	}
	
	/**
	 * 字串格式轉換為 :數字右靠左補0
	 * 
	 * @param numStr : 需更改格式的字串
	 * @param size : 要轉換的長度
	 * @return : 回傳轉換後的
	 * @throws Exception
	 */
	public static String formatNumber(String numStr, int size) throws Exception {
		if (numStr == null) {
			throw new Exception("字串不可為NULL");
		}

		if (numStr.length() > size) {
			throw new Exception("長度不合");
		}

		String zero = "";
		for (int i = 0; i < (size - numStr.length()); i++) {
			zero = zero + "0";
		}
		return zero + numStr;
	}

	/**
	 * 字串轉BigDecimal 預設格式"000000000000.00"
	 * 
	 * @param strDecimal : 進行轉換字串
	 * @return : 回傳轉換後的
	 * @throws Exception
	 */
	public static BigDecimal strToBigDecimal(String strDecimal) throws ParseException {
		DecimalFormatSymbols symbols = new DecimalFormatSymbols();
		symbols.setDecimalSeparator('.');
		String pattern = "000000000000.00";
		DecimalFormat decimalFormat = new DecimalFormat(pattern, symbols);
		decimalFormat.setParseBigDecimal(true);
		return ((BigDecimal) decimalFormat.parse(strDecimal));
	}

	/**
	 * 字串轉BigDecimal 預設整數位為12位
	 * 
	 * @param strDecimal : 進行轉換字串
	 * @param newScale 希望擁有幾位的小數位
	 * @return 轉換後的BigDecimal
	 * @throws ParseException
	 */
	public static BigDecimal strToBigDecimal(String strDecimal, int newScale) throws ParseException {
		if (ETL_Tool_FormatCheck.isEmpty(strDecimal)) {
			return strToBigDecimal("0");
		}
		
		try {
			strDecimal = strDecimal.substring(0, strDecimal.length() - newScale) + "."
					+ strDecimal.substring(strDecimal.length() - newScale, strDecimal.length());
	
			return strToBigDecimal(strDecimal);
		} catch (Exception ex) {
			return strToBigDecimal("0");
		}
	}

	/**
	 * 字串轉BigDecimal
	 * @param strDecimal : 進行轉換字串
	 * @param pattern : 規定的格式
	 * @return :回傳轉換後的BigDecimal
	 * @throws ParseException
	 */
	public static BigDecimal strToBigDecimal(String strDecimal, String pattern) throws ParseException {
		DecimalFormatSymbols symbols = new DecimalFormatSymbols();
		symbols.setDecimalSeparator('.');
		DecimalFormat decimalFormat = new DecimalFormat(pattern, symbols);
		decimalFormat.setParseBigDecimal(true);
		return ((BigDecimal) decimalFormat.parse(strDecimal));
	}

	/**
	 * BigDecimal 轉 字串 預設格式"#.#"
	 * 
	 * @param bigDecimal :要轉換的BigDecimal
	 * @return 回傳轉換後的bigDecimal
	 * @throws Exception
	 */
	public static String bigDecimalToStr(BigDecimal bigDecimal) throws Exception {
		if (bigDecimal == null) {
			throw new Exception("BigDecimal不可為NULL");
		}

		DecimalFormat df = new DecimalFormat("#.00");
		return df.format(bigDecimal);
	}

	/**
	 * 字串格式轉換為:文字左靠右補空白
	 * 
	 * @param str :字串
	 * @param size :全部長度
	 * @return :回傳轉換後的
	 * @throws Exception
	 */
	public static String FormatString(String str, int size) throws Exception {

		if (str == null) {
			throw new Exception("字串不可為NULL");
		}

		if (str.length() > size) {
			throw new Exception("長度不合");
		}

		byte[] byteArr = str.getBytes();
		byte[] formByteArr = new byte[size];

		for (int i = 0; i < size; i++) {
			formByteArr[i] = 32;
		}

		System.arraycopy(byteArr, 0, formByteArr, 0, byteArr.length);
		String formStr = new String(formByteArr);

		return formStr;
	}

	/**
	 * 字串轉為數字
	 * @param str:字串
	 * @return 為正確數字則回傳int型態, 否則回傳0
	 */
	public static int toInt(String str) {
		if (ETL_Tool_FormatCheck.checkNum(str)) {
			return Integer.valueOf(str);
		} else {
			return 0;
		}
	}
	
	/**
	 * 字串轉為數字
	 * @param str:字串
	 * @return 為正確數字則回傳int型態, 否則回傳0
	 */
	public static long toLong(String str) {
		if (ETL_Tool_FormatCheck.checkNum(str)) {
			return Long.parseLong(str);
		} else {
			return 0;
		}
	}
	
//	public static void main(String[] argv) throws Exception {
//		Timestamp time =toTimestamp("163310","HHmmss");
//		long aaa=time.getTime();
//		
//		Timestamp time1=new Timestamp(aaa);
//		
//	
//		Date date = new Date();
//		date.setTime(time1.getTime());
//		String formattedDate = new SimpleDateFormat("HHmmss").format(date);
//		
//		System.out.println(formattedDate);
//		
//	}

}
