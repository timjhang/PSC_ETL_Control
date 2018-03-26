package Tool;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
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
	public static String format(byte[] stream, Map<String, Map<String, String>> difficultWordMaps) throws IOException {

		// 拿到Big自訂字碼與Unicode自訂字碼的Map
		Map<String, String> pp_map = difficultWordMaps.get("pp_map");

		// 拿到Big自訂字碼與Unicode系統字碼的Map
		Map<String, String> ps_map = difficultWordMaps.get("ps_map");

		int prevIndex = -1;
		int size = stream.length;

		StringBuffer sBuffer = new StringBuffer();

		for (int i = 0; i < size; i++) {

			if (i != 0 && i < size) {
				byte[] b = { stream[prevIndex], stream[i] };

				String code = byteArrayToHexStr(b);
				
				boolean isBig5Code = isBig5Code(code);
				boolean isBig5DifficultWord = isBig5DifficultWord(code);

				i += isBig5Code | isBig5DifficultWord ? 1 : 0;

				if (isBig5DifficultWord) {
					// 查找UniCode系統字區
					String mapped_code = ps_map.get(code);

					// 判斷系統字區是否有對應的字碼
					if (mapped_code == null || "".equals(mapped_code.trim())) {

						// 查找UniCode造字區
						mapped_code = pp_map.get(code);

						// 判斷造字區是否有對應的字碼，假如沒有，則回傳特殊字碼 □
						mapped_code = (mapped_code == null || "".equals(mapped_code.trim())) ? "25a1" : mapped_code;

					}
					sBuffer.append(UTF_8(mapped_code));
				} else if (isBig5Code) {
					sBuffer.append(new String(b, "big5"));
				} else {
					sBuffer.append(new String(b, 0, 1));
				}
			}
			prevIndex = i;
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
	private static String UTF_8(String unicode) throws UnsupportedEncodingException {
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

		// try {
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

		// try {
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
		return map;
	}

	/**
	 * 把byte array 轉換成16進位字串
	 * 
	 * @param src
	 *            來源陣列
	 * @return 16進位字串
	 */
	private static String byteArrayToHexStr(byte[] byteArray) {
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
	 * 造字範圍 字數 FA40-FEFE, 8E40-A0FE, 8140-8DFE, C6A1-C8FE
	 */
	private static boolean isBig5DifficultWord(String code) {
		boolean is = false;

		try {
			int decimal = Integer.parseInt(code, 16);

			int range_1_start = Integer.parseInt("FA40", 16);
			int range_1_end = Integer.parseInt("FEFE", 16);

			int range_2_start = Integer.parseInt("8E40", 16);
			int range_2_end = Integer.parseInt("A0FE", 16);

			int range_3_start = Integer.parseInt("8140", 16);
			int range_3_end = Integer.parseInt("8DFE", 16);

			int range_4_start = Integer.parseInt("C6A1", 16);
			int range_4_end = Integer.parseInt("C8FE", 16);

			if (((range_1_start <= decimal) && (decimal <= range_1_end))
					| ((range_2_start <= decimal) && (decimal <= range_2_end))
					| ((range_3_start <= decimal) && (decimal <= range_3_end))
					| ((range_4_start <= decimal) && (decimal <= range_4_end)))
				is = true;
		} catch (Exception e) {
			return is;
		}

		return is;
	}

	/**
	 * 判斷字碼是否存在於Big5中
	 * 
	 * @param code
	 *            HEX字碼
	 * @return
	 */
	private static boolean isBig5Code(String code) {
		boolean is = false;

		if (!ETL_Tool_FormatCheck.isEmpty(code) && code.length() == 4) {

			char code_1, code_2, code_3, code_4;
			code_1 = code.charAt(0);
			code_2 = code.charAt(1);
			code_3 = code.charAt(2);
			code_4 = code.charAt(3);

			if ('A' == code_1 || 'B' == code_1 || 'C' == code_1 || 'D' == code_1 || 'E' == code_1 || 'F' == code_1) {

				if ((('A' == code_1) && ('1' == code_2 || '2' == code_2 || '3' == code_2 || '4' == code_2
						|| '5' == code_2 || '6' == code_2 || '7' == code_2 || '8' == code_2 || '9' == code_2
						|| '9' == code_2 || 'A' == code_2 || 'B' == code_2 || 'C' == code_2 || 'D' == code_2
						|| 'E' == code_2 || 'F' == code_2))

						||

						(('F' == code_1) && ('0' == code_2 || '1' == code_2 || '2' == code_2 || '3' == code_2
								|| '4' == code_2 || '5' == code_2 || '6' == code_2 || '7' == code_2 || '8' == code_2
								|| '9' == code_2))

						||

						(('A' != code_1) && ('F' != code_1)
								&& ('0' == code_2 || '1' == code_2 || '2' == code_2 || '3' == code_2 || '4' == code_2
										|| '5' == code_2 || '6' == code_2 || '7' == code_2 || '8' == code_2
										|| '9' == code_2 || 'A' == code_2 || 'B' == code_2 || 'C' == code_2
										|| 'D' == code_2 || 'E' == code_2 || 'F' == code_2))

				) {

					if ('4' == code_3 || '5' == code_3 || '6' == code_3 || '7' == code_3 || 'A' == code_3
							|| 'B' == code_3 || 'C' == code_3 || 'D' == code_3 || 'E' == code_3 || 'F' == code_3) {

						if ('0' == code_4 || '1' == code_4 || '2' == code_4 || '3' == code_4 || '4' == code_4
								|| '5' == code_4 || '6' == code_4 || '7' == code_4 || '8' == code_4 || '9' == code_4
								|| '9' == code_4 || 'A' == code_4 || 'B' == code_4 || 'C' == code_4 || 'D' == code_4
								|| 'E' == code_4 || 'F' == code_4

						) {
							is = true;
						}
					}

				}
			}
		}
		return is;
	}

	public static void main(String[] args) throws Exception {
		ETL_Tool_Big5_To_UTF8 test = new ETL_Tool_Big5_To_UTF8("D:/PSC/Projects/AgriBank/難字/難字轉換表/%s.xlsx");
		Map<String, Map<String, String>> difficultWordMaps = test.getDifficultWordMaps("952");

		byte[] bytes = Files.readAllBytes(Paths.get("D:\\PSC\\Projects\\AgriBank\\UNIT_TEST\\600_R_TRANSACTION_20171206.TXT"));
		
		System.out.println(ETL_Tool_Big5_To_UTF8.format(bytes, difficultWordMaps));
	}
}
