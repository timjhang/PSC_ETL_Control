package Tool;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import Profile.ETL_Profile;

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
			e.printStackTrace();
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
	public static String format(byte[] stream, Map<String, Map<String, String>> difficultWordMaps,
			Map<String, String> specialBig5Map) throws IOException {
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

				// 符合Big5正常編碼條件，就進行直接轉換，但會先判別是否為罕見字
				if (isBig5Code) {
					sBuffer.append(isBig5CodeSpecial(code) ? transFormBig5CodeSpecial(b, specialBig5Map)
							: new String(b, "big5"));
					// 如果是ASCII編碼，則進入條件
				} else if (isAllASCII(new String(b, 0, 1))) {
					sBuffer.append(new String(b, 0, 1));
					i -= 1;
					/*
					 * 判斷對照表中，是否含有對照資訊，pp_map
					 * 和ps_map，擇一進行比對即可，以Big自訂字碼為比對依據，假如不存在即進入條件
					 */
				} else if (!ps_map.containsKey(code)) {
					// 符合條件則回傳特殊字碼 □
					sBuffer.append(UTF_8("25a1"));
					// 其餘皆以EXCEL去處理
				} else {
					// 查找UniCode系統字區
					String mapped_code = ps_map.get(code);
					mapped_code = allTrim(mapped_code);
					
					// 判斷系統字區是否有對應的字碼
					if (mapped_code == null || "".equals(mapped_code) || "x".equalsIgnoreCase(mapped_code)) {

						// 查找UniCode造字區
						mapped_code = pp_map.get(code);

						// 判斷造字區是否有對應的字碼，假如沒有，則回傳特殊字碼 □
						mapped_code = (mapped_code == null || "".equals(mapped_code)
								|| "x".equalsIgnoreCase(mapped_code)) ? "25a1" : mapped_code;

					}
					sBuffer.append(UTF_8(mapped_code));
				}
				i += 1;
			}
			prevIndex = i;
		}

		return sBuffer.toString();
	}

	// /**
	// * 依照農金提供的對照表，進行Big5到UTF-8的轉換
	// *
	// * @param stream
	// * 要進行轉換的byte array，編碼為Big5
	// * @param difficultWordMaps
	// * 相關對照的資訊表
	// * @return 轉換後的UTF-8字串
	// * @throws IOException
	// */
	// public static String format(byte[] stream, Map<String, Map<String,
	// String>> difficultWordMaps,
	// Map<String, String> specialBig5Map) throws IOException {
	// // 拿到Big自訂字碼與Unicode自訂字碼的Map
	// Map<String, String> pp_map = difficultWordMaps.get("pp_map");
	//
	// // 拿到Big自訂字碼與Unicode系統字碼的Map
	// Map<String, String> ps_map = difficultWordMaps.get("ps_map");
	//
	// int prevIndex = -1;
	// int size = stream.length;
	//
	// StringBuffer sBuffer = new StringBuffer();
	//
	// for (int i = 0; i < size; i++) {
	//
	// if (i != 0 && i < size) {
	// byte[] b = { stream[prevIndex], stream[i] };
	//
	// String code = byteArrayToHexStr(b);
	//
	// // System.out.println("code: " + code);
	//
	// boolean isBig5Code = isBig5Code(code);
	// boolean isBig5DifficultWord = isBig5DifficultWord(code);
	//
	// // System.out.println("isBig5Code: " + isBig5Code + " \\
	// // isBig5DifficultWord: " + isBig5DifficultWord);
	//
	// i += isBig5Code | isBig5DifficultWord ? 1 : 0;
	//
	// if (isBig5DifficultWord) {
	// // 查找UniCode系統字區
	// String mapped_code = ps_map.get(code);
	//
	// // System.out.println("查找UniCode系統字區: " + mapped_code);
	//
	// // 判斷系統字區是否有對應的字碼
	// if (mapped_code == null || "".equals(mapped_code.trim())) {
	// // System.out.println("查無系統字區，改為查詢造字區");
	//
	// // 查找UniCode造字區
	// mapped_code = pp_map.get(code);
	//
	// // System.out.println("查找UniCode造字區: " + mapped_code);
	//
	// // 判斷造字區是否有對應的字碼，假如沒有，則回傳特殊字碼 □
	// mapped_code = (mapped_code == null || "".equals(mapped_code.replaceAll("[
	// | ]", ""))
	// || "x".equalsIgnoreCase(mapped_code.replaceAll("[ | ]", ""))) ? "25a1" :
	// mapped_code;
	//
	// }
	// sBuffer.append(UTF_8(mapped_code));
	// } else if (isBig5Code) {
	//
	// sBuffer.append(isBig5CodeSpecial(code) ? transFormBig5CodeSpecial(b,
	// specialBig5Map)
	// : new String(b, "big5"));
	// } else {
	// sBuffer.append(new String(b, 0, 1));
	// }
	// }
	// prevIndex = i;
	// }
	//
	// return sBuffer.toString();
	// }

	/**
	 * 轉換Unicode為相對應的UTF-8文字
	 * 
	 * @param unicode
	 *            要進行轉換的Unicode字碼
	 * @return 相對應的文字，型態為UTF-8
	 * @throws UnsupportedEncodingException
	 */
	private static String UTF_8(String unicode) throws UnsupportedEncodingException {
		return new String(Character.toChars(Integer.parseInt(unicode, 16)));
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

			if (transCell(big5_cell) != null) {
				// UniCode系統字區
				Cell uniCode_cell = currentRow.getCell(3);

				map.put(transCell(big5_cell), transCell(uniCode_cell));
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

			if (transCell(big5_cell) != null) {
				// UniCode造字區
				Cell uniCode_cell = currentRow.getCell(2);

				map.put(transCell(big5_cell), transCell(uniCode_cell));
			}
		}
		return map;
	}

	/**
	 * 轉換Cell值，目前只做字串和數字規則
	 * 
	 * @param cell
	 * @return 字串型態
	 */
	private String transCell(Cell cell) {

		String cell_val = null;

		try {
			if (cell.getCellTypeEnum() == CellType.STRING) {
				cell_val = cell.getStringCellValue();
				if (!ETL_Tool_FormatCheck.isEmpty(cell_val))
					cell_val = cell_val.replaceAll("[ |　]", "");
			}

			if (cell.getCellTypeEnum() == CellType.NUMERIC) {
				Pattern pattern = Pattern.compile("(\\d+)");
				Matcher matcher = pattern.matcher(String.valueOf(cell.getNumericCellValue()));
				if (matcher.find())
					cell_val = matcher.group(1);
			}
		} catch (Exception e) {
			return cell_val;
		}

		return cell_val;
	}

	// 擴充字及特殊符號補充表
	public Map<String, String> get_Special_Big5_System_And_Unicode_System_Map() {

		Map<String, String> map = null;

		try {
			map = new HashMap<String, String>();

			FileInputStream excelFile = new FileInputStream(new File(ETL_Profile.SpecialWords_Lists_Path));

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

				// Big5內碼區
				Cell big5_cell = currentRow.getCell(1);

				if (big5_cell != null) {
					// UniCode內碼區
					Cell uniCode_cell = currentRow.getCell(2);
					String uniCode_cell_val = uniCode_cell == null ? null : uniCode_cell.getStringCellValue();
					map.put(big5_cell.getStringCellValue(), uniCode_cell_val);
				}
			}

		} catch (Exception e) {
			return null;
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

	/**
	 * 判斷內碼是否為big5中七個罕見字之一(碁恒裏粧嫺銹墻)
	 * 
	 * @param code
	 *            big5內碼
	 * @return true/false : 是/不是
	 */
	private static boolean isBig5CodeSpecial7(String code) {
		boolean isBig5CodeSpecial7 = false;

		if (ETL_Tool_FormatCheck.isEmpty(code) || code.length() < 4)
			return isBig5CodeSpecial7;

		if (!code.toUpperCase().startsWith("F9D"))
			return isBig5CodeSpecial7;

		int codeInt = Integer.parseInt(code, 16);
		if (63958 <= codeInt && codeInt <= 63964)
			isBig5CodeSpecial7 = true;

		return isBig5CodeSpecial7;
	}

	/**
	 * 轉換處理big5中七個罕見字(碁恒裏粧嫺銹墻)
	 * 
	 * @param bytes
	 *            罕見字的byte array
	 * @return 轉換處理好後的unicode字
	 * @throws UnsupportedEncodingException
	 */
	private static String transFormBig5CodeSpecial7(byte[] bytes) throws UnsupportedEncodingException {
		String code = byteArrayToHexStr(bytes);

		Map<String, String> hexMap = new HashMap<String, String>();

		// big5,unicode
		hexMap.put("F9D6", "7881");
		hexMap.put("F9D7", "92B9");
		hexMap.put("F9D8", "88CF");
		hexMap.put("F9D9", "58BB");
		hexMap.put("F9DA", "6052");
		hexMap.put("F9DB", "7CA7");
		hexMap.put("F9DC", "5AFA");

		String unicode = hexMap.get(code);

		return unicode == null ? null : UTF_8(unicode);
	}

	/*
	 * 特殊符號及罕見字區間
	 * 
	 * A140:41280 A17E:41342
	 * 
	 * A1A1:41377 A1FE:41470
	 * 
	 * A240:41536 A27E:41598
	 * 
	 * A2A1:41633 A2FE:41726
	 * 
	 * A340:41792 A37E:41854
	 * 
	 * A3A1:41889 A3BF:41919
	 * 
	 * A3E1:41953
	 * 
	 * F9D6:63958 F9FE:63998
	 */
	/**
	 * 判斷內碼是否為big5中罕見字及特殊符號
	 * 
	 * @param code
	 *            big5內碼
	 * @return true/false : 是/不是
	 */
	private static boolean isBig5CodeSpecial(String code) {
		boolean isBig5CodeSpecial = false;

		if (ETL_Tool_FormatCheck.isEmpty(code) || code.length() < 4)
			return isBig5CodeSpecial;

		int codeInt = Integer.parseInt(code, 16);
		if (codeInt < 41280)
			return isBig5CodeSpecial;

		if (41280 <= codeInt && codeInt <= 41342)
			isBig5CodeSpecial = true;

		if (41377 <= codeInt && codeInt <= 41470)
			isBig5CodeSpecial = true;

		if (41536 <= codeInt && codeInt <= 41598)
			isBig5CodeSpecial = true;

		if (41633 <= codeInt && codeInt <= 41726)
			isBig5CodeSpecial = true;

		if (41792 <= codeInt && codeInt <= 41854)
			isBig5CodeSpecial = true;

		if (41889 <= codeInt && codeInt <= 41919)
			isBig5CodeSpecial = true;

		if (41953 == codeInt)
			isBig5CodeSpecial = true;

		if (63958 <= codeInt && codeInt <= 63998)
			isBig5CodeSpecial = true;

		return isBig5CodeSpecial;
	}

	/**
	 * 轉換處理big5中罕見字及特殊符號
	 * 
	 * @param bytes
	 *            罕見字及特殊符號的byte array
	 * @return 轉換處理好後的unicode字，假如擴充表不存在的話，則直接硬轉
	 * @throws IOException
	 */
	private static String transFormBig5CodeSpecial(byte[] bytes, Map<String, String> specialBig5Map)
			throws IOException {
		String code = byteArrayToHexStr(bytes);

		String unicode = specialBig5Map.get(code);

		return specialBig5Map == null | unicode == null ? UTF_8(byteArrayToHexStr(bytes)) : UTF_8(unicode);
	}

	private void print_is_Big5_excel_fromat(String central_no) throws IOException {

		Map<String, String> map = new HashMap<String, String>();

		// try {
		String XLSXPath = getXLSXPath(central_no);

		FileInputStream excelFile = new FileInputStream(new File(XLSXPath));

		@SuppressWarnings("resource")
		Workbook workbook = new XSSFWorkbook(excelFile);

		// 取出第一個工作表
		Sheet datatypeSheet = workbook.getSheetAt(0);

		boolean is = true;

		for (int i = 1; i <= datatypeSheet.getLastRowNum(); i++) {

			Row currentRow = datatypeSheet.getRow(i);

			// 為空，不處理
			if (currentRow == null) {
				continue;
			}

			// Big5造字區
			Cell big5_cell = currentRow.getCell(1);

			if (transCell(big5_cell) != null) {

				if (!isBig5Code(transCell(big5_cell)) && !isBig5DifficultWord(transCell(big5_cell))) {
					is = false;
					System.out.println("第" + (i + 1) + "行，Big5造字區非Big5編碼");
				}
			}

		}
		if (is)
			System.out.println("Big5造字區全為Big5編碼");

	}

	private static boolean isAllASCII(String input) {
		boolean isASCII = true;
		for (int i = 0; i < input.length(); i++) {
			int c = input.charAt(i);
			if (c > 0x7F) {
				isASCII = false;
				break;
			}
		}
		return isASCII;
	}

	private static String allTrim(String s) {
		return s == null ? null : s.replaceAll("[ |　]", "");
	}

	public static void main(String[] args) throws IOException {
		ETL_Tool_Big5_To_UTF8 wordsXTool = new ETL_Tool_Big5_To_UTF8(ETL_Profile.DifficultWords_Lists_Path);

		Map<String, Map<String, String>> difficultWordMaps = wordsXTool.getDifficultWordMaps("910");
		Map<String, String> specialBig5Map = wordsXTool.get_Special_Big5_System_And_Unicode_System_Map();

		String uri = "D:\\PSC\\Projects\\AgriBank\\UNIT_TEST\\字碼測試.TXT";

		byte[] stream = Files.readAllBytes(Paths.get(uri));
		System.out.println(byteArrayToHexStr(stream));
		System.out.println(format(stream, difficultWordMaps, specialBig5Map));

		// wordsXTool.print_is_Big5_excel_fromat("600");
		// System.out.println(isBig5Code("1111"));
		// System.out.println(isBig5CodeSpecial("1111"));
		// System.out.println(isBig5DifficultWord("1111"));

		// System.out.println(Integer.parseInt("F", 16));
		// System.out.println("f".getBytes("big5").length);
		System.out.println(byteArrayToHexStr("".getBytes()));

		// System.out.println(isAllASCII("我"));
	}
}
