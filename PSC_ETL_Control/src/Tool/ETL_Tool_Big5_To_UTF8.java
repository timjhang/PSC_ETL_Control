package Tool;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class ETL_Tool_Big5_To_UTF8 {

	// 存放於指定目錄的xlsx格式化參數
	private String XLSXCondition;

	public ETL_Tool_Big5_To_UTF8(String xLSXCondition) {
		XLSXCondition = xLSXCondition;
	}

	/**
	 * 依據報送單位代碼，拿到相關的對照Maps
	 * 
	 * @param central_no
	 *            報送單位
	 * @return 相關的Map
	 */
	public Map<String, Map<String, String>> getDifficultWordMaps(String central_no) {
		Map<String, Map<String, String>> map = null;

		try {
			// 實體化
			map = new HashMap<String, Map<String, String>>();

			// 拿到Big自訂字碼與Unicode自訂字碼的Map
			Map<String, String> pp_map = get_Big5_Provision_And_Unicode_Provision_Map(central_no);

			// 拿到Big自訂字碼與Unicode系統字碼的Map
			Map<String, String> ps_map = get_Big5_Provision_And_Unicode_System_Map(central_no);

			map.put("pp_map", pp_map);
			map.put("ps_map", ps_map);

		} catch (Exception e) {
			return null;
		}

		return map;
	}

	/**
	 * 依照農金提供的對照表，進行Big5到UTF-8的轉換
	 * 
	 * @param stream
	 *            要進行轉換的byte array，編碼為Big5
	 * @param difficultWordMaps
	 *            相關對照的資訊表
	 * @return 轉換後的UTF-8字串
	 * @throws IOException
	 */
	public String format(byte[] stream, Map<String, Map<String, String>> difficultWordMaps) throws IOException {

		// 拿到來源字碼
		String code = byteArrayToHexStr(stream);

		// 拿到Big自訂字碼與Unicode自訂字碼的Map
		Map<String, String> pp_map = difficultWordMaps.get("pp_map");

		// 拿到Big自訂字碼與Unicode系統字碼的Map
		Map<String, String> ps_map = difficultWordMaps.get("ps_map");

		StringBuffer sBuffer = new StringBuffer();

		int count = 0;

		for (int i = 0; i < code.length(); i += 4) {
			String tmp = code.substring(i, i + 4);
			// logger.debug("進行解析，目前字碼: {}", tmp);

			boolean b = isBig5DifficultWord(tmp);
			// logger.debug("是否為造字區造字: {}", b ? "是" : "不是");

			if (b) {
				// 首先查找UniCode系統字區
				String mapped_code = ps_map.get(tmp);

				// 判斷系統字區是否有對應的字碼
				if (mapped_code == null || "".equals(mapped_code.trim())) {

					// 查找UniCode造字區
					mapped_code = pp_map.get(tmp);

					// 判斷造字區是否有對應的字碼，假如沒有，則回傳特殊字碼
					mapped_code = (mapped_code == null || "".equals(mapped_code.trim())) ? "25a1" : mapped_code;

				}
				sBuffer.append(UTF_8(mapped_code));
			} else {
				sBuffer.append(new String(Arrays.copyOfRange(stream, count, count + 2), "big5"));
			}
			count += 2;
		}
		return sBuffer.toString();
	}

	/**
	 * 轉換Unicode為相對應的UTF-8文字
	 * 
	 * @param unicode
	 *            要進行轉換的Unicode字碼
	 * @return 相對應的文字，型態為UTF-8
	 * @throws UnsupportedEncodingException
	 */
	private String UTF_8(String unicode) throws UnsupportedEncodingException {
		String unicodeStr = "";
		int hexVal = Integer.parseInt(unicode, 16);
		unicodeStr += (char) hexVal;
		return new String(unicodeStr.getBytes("UTF-8"), StandardCharsets.UTF_8);
	}

	/**
	 * 得到EXCEL路徑
	 * 
	 * @param central_no
	 *            報送單位
	 * @return 格式化後的路徑
	 */
	private String getXLSXPath(String central_no) {
		StringBuffer stringBuffer = new StringBuffer();
		@SuppressWarnings("resource")
		Formatter formatter = new Formatter(stringBuffer);
		formatter.format(XLSXCondition, central_no);
		return stringBuffer.toString();
	}

	/**
	 * 讀取EXCEL檔案，拿到Big自訂字碼與Unicode系統字碼的Map
	 * 
	 * @param XLSXPath
	 *            來源EXCEL路徑
	 * @return key是Big5自訂字碼，Value是Unicode系統字的Map
	 * @throws IOException
	 */
	private Map<String, String> get_Big5_Provision_And_Unicode_System_Map(String central_no) throws IOException {

		Map<String, String> map = new HashMap<String, String>();

//		try {
			String XLSXPath = getXLSXPath(central_no);

			FileInputStream excelFile = new FileInputStream(new File(XLSXPath));

			@SuppressWarnings("resource")
			Workbook workbook = new XSSFWorkbook(excelFile);

			// 取出第一個工作表
			Sheet datatypeSheet = workbook.getSheetAt(0);

			for (int i = 1; i <= datatypeSheet.getLastRowNum(); i++) {

				Row currentRow = datatypeSheet.getRow(i);

				// 為空，不處理
				if (currentRow == null) {
					continue;
				}

				// Big5造字區
				Cell big5_cell = currentRow.getCell(1);

				if (big5_cell != null) {
					// UniCode系統字區
					Cell uniCode_cell = currentRow.getCell(3);
					String uniCode_cell_val = uniCode_cell == null ? null : uniCode_cell.getStringCellValue();
					map.put(big5_cell.getStringCellValue(), uniCode_cell_val);
				}
			}
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		return map;
	}

	/**
	 * 讀取EXCEL檔案，拿到Big自訂字碼與Unicode自訂字碼的Map
	 * 
	 * @param XLSXPath
	 *            來源EXCEL路徑
	 * @return key是Big5自訂字碼，Value是Unicode自訂字碼的Map
	 * @throws IOException
	 */
	private Map<String, String> get_Big5_Provision_And_Unicode_Provision_Map(String central_no) throws IOException {

		Map<String, String> map = new HashMap<String, String>();

//		try {
			String XLSXPath = getXLSXPath(central_no);

			FileInputStream excelFile = new FileInputStream(new File(XLSXPath));

			@SuppressWarnings("resource")
			Workbook workbook = new XSSFWorkbook(excelFile);

			// 取出第一個工作表
			Sheet datatypeSheet = workbook.getSheetAt(0);

			for (int i = 1; i <= datatypeSheet.getLastRowNum(); i++) {

				Row currentRow = datatypeSheet.getRow(i);

				// 為空，不處理
				if (currentRow == null) {
					continue;
				}

				// Big5造字區
				Cell big5_cell = currentRow.getCell(1);

				if (big5_cell != null) {
					// UniCode造字區
					Cell uniCode_cell = currentRow.getCell(2);
					String uniCode_cell_val = uniCode_cell == null ? null : uniCode_cell.getStringCellValue();
					map.put(big5_cell.getStringCellValue(), uniCode_cell_val);
				}
			}
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		return map;
	}

	/**
	 * 把byte array 轉換成16進位字串
	 * 
	 * @param src
	 *            來源陣列
	 * @return 16進位字串
	 */
	private String byteArrayToHexStr(byte[] byteArray) {
		if (byteArray == null) {
			return null;
		}
		char[] hexArray = "0123456789ABCDEF".toCharArray();
		char[] hexChars = new char[byteArray.length * 2];
		for (int j = 0; j < byteArray.length; j++) {
			int v = byteArray[j] & 0xFF;
			hexChars[j * 2] = hexArray[v >>> 4];
			hexChars[j * 2 + 1] = hexArray[v & 0x0F];
		}
		return new String(hexChars);
	}

	/**
	 * 判斷字碼是否存在於Big5造字區中
	 * 
	 * @param code
	 *            Big5字碼
	 * @return trur 存在 / false 不存在
	 */
	/*
	 * 造字範圍 字數 FA40-FEFE 785 8E40-A0FE 2983 8140-8DFE 2041
	 */
	private boolean isBig5DifficultWord(String code) {
		boolean is = false;

		try {
			int decimal = Integer.parseInt(code, 16);

			int range_1_start = Integer.parseInt("FA40", 16);
			int range_1_end = Integer.parseInt("FEFE", 16);

			int range_2_start = Integer.parseInt("8E40", 16);
			int range_2_end = Integer.parseInt("A0FE", 16);

			int range_3_start = Integer.parseInt("8140", 16);
			int range_3_end = Integer.parseInt("8DFE", 16);

			if (((range_1_start <= decimal) && (decimal <= range_1_end))
					| ((range_2_start <= decimal) && (decimal <= range_2_end))
					| ((range_3_start <= decimal) && (decimal <= range_3_end)))
				is = true;
		} catch (Exception e) {
			return is;
		}

		return is;
	}
}
