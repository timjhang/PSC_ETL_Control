package Control;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import Bean.ETL_Bean_CLP_Script;
import DB.ConnectionHelper;
import Profile.ETL_Profile;
import Tool.ETL_Tool_StringX;

public class ETL_C_FIVE_G {
	
	// 執行產生新一代Table
	public static boolean generateNewGTable(Date oldDate, Date newDate, String central_No, String tableType) {
		// tableType 預定有兩種 temp, rerun(temp:正常ETL使用, rerun:Rerun使用)
		
		try {
			// 寫入5代Table 記錄檔
			if (!writeStartGenerationStatus(newDate, central_No)) {
				System.out.println("####ETL_C_FIVE_G - 寫入NewGenerationStatus紀錄失敗，不繼續作業!!");
				return false;
			}
			
			// Transaction超過一定門檻時執行reorg(產生新一代後即可能進行ETL作業, 故先進行可能的reorg)
			// 但reorg時間可能達45分鐘之久, 務必需要進行註冊, 產新一代作業已經開始。
			reorgTransaction(central_No, oldDate);
				
			if (!detachTablePartiton(central_No, newDate)) {
				throw new Exception("detach partition " + central_No + " " + new SimpleDateFormat("yyyyMMdd").format(newDate) + " 出現錯誤!");
			}

			// 產生新一代Table Script
			createCopyTableCLPScript(oldDate, newDate, central_No, "ACCOUNT", tableType);
			createCopyTableCLPScript(oldDate, newDate, central_No, "ACCOUNT_PROPERTY", tableType);
			createCopyTableCLPScript(oldDate, newDate, central_No, "BALANCE", tableType);
			createCopyTableCLPScript(oldDate, newDate, central_No, "LOAN", tableType);
			createCopyTableCLPScript(oldDate, newDate, central_No, "LOAN_COLLATERAL", tableType);
			createCopyTableCLPScript(oldDate, newDate, central_No, "LOAN_DETAIL", tableType);
			createCopyTableCLPScript(oldDate, newDate, central_No, "LOAN_GUARANTOR", tableType);
			createCopyTableCLPScript(oldDate, newDate, central_No, "LOAN_MASTER", tableType);
			createCopyTableCLPScript(oldDate, newDate, central_No, "PARTY", tableType);
			createCopyTableCLPScript(oldDate, newDate, central_No, "PARTY_ACCOUNT_REL", tableType);
			createCopyTableCLPScript(oldDate, newDate, central_No, "PARTY_ADDRESS", tableType);
			createCopyTableCLPScript(oldDate, newDate, central_No, "PARTY_EMAIL", tableType);
//			createCopyTableCLPScript(oldDate, newDate, central_No, "PARTY_NATIONALITY", tableType);
			createCopyTableCLPScript(oldDate, newDate, central_No, "PARTY_PARTY_REL", tableType);
			createCopyTableCLPScript(oldDate, newDate, central_No, "PARTY_PHONE", tableType);
				
			// 執行Script
			runCLPScript(central_No, "GENERATE", "ACCOUNT", tableType);
			runCLPScript(central_No, "GENERATE", "ACCOUNT_PROPERTY", tableType);
			runCLPScript(central_No, "GENERATE", "BALANCE", tableType);
			runCLPScript(central_No, "GENERATE", "LOAN", tableType);
			runCLPScript(central_No, "GENERATE", "LOAN_COLLATERAL", tableType);
			runCLPScript(central_No, "GENERATE", "LOAN_DETAIL", tableType);
			runCLPScript(central_No, "GENERATE", "LOAN_GUARANTOR", tableType);
			runCLPScript(central_No, "GENERATE", "LOAN_MASTER", tableType);
			runCLPScript(central_No, "GENERATE", "PARTY", tableType);
			runCLPScript(central_No, "GENERATE", "PARTY_ACCOUNT_REL", tableType);
			runCLPScript(central_No, "GENERATE", "PARTY_ADDRESS", tableType);
			runCLPScript(central_No, "GENERATE", "PARTY_EMAIL", tableType);
//			runCLPScript(central_No, "GENERATE", "PARTY_NATIONALITY", tableType);
			runCLPScript(central_No, "GENERATE", "PARTY_PARTY_REL", tableType);
			runCLPScript(central_No, "GENERATE", "PARTY_PHONE", tableType);
			
			// 更新5代Table 記錄檔
			updateNewGenerationPrepareStatus(newDate, central_No, "End", "");
				
		} catch (Exception ex) {
			ex.printStackTrace();
			updateNewGenerationPrepareStatus(newDate, central_No, "Error", ex.getMessage());
			return false;
		}
		
		return true;
	}
	
	// 除舊佈新五代Table
	public static boolean renew5GTable(Date dropDate, Date newDate, String central_No, String tableType) {
		// tableType 預定有兩種 temp, rerun(temp:正常ETL使用, rerun:Rerun使用)
		
		try {
			// 產生新一代Table Script
			createPasteTableCLPScript(dropDate, newDate, central_No, "ACCOUNT", tableType);
			createPasteTableCLPScript(dropDate, newDate, central_No, "ACCOUNT_PROPERTY", tableType);
			createPasteTableCLPScript(dropDate, newDate, central_No, "BALANCE", tableType);
			createPasteTableCLPScript(dropDate, newDate, central_No, "LOAN", tableType);
			createPasteTableCLPScript(dropDate, newDate, central_No, "LOAN_COLLATERAL", tableType);
			createPasteTableCLPScript(dropDate, newDate, central_No, "LOAN_DETAIL", tableType);
			createPasteTableCLPScript(dropDate, newDate, central_No, "LOAN_GUARANTOR", tableType);
			createPasteTableCLPScript(dropDate, newDate, central_No, "LOAN_MASTER", tableType);
			createPasteTableCLPScript(dropDate, newDate, central_No, "PARTY", tableType);
			createPasteTableCLPScript(dropDate, newDate, central_No, "PARTY_ACCOUNT_REL", tableType);
			createPasteTableCLPScript(dropDate, newDate, central_No, "PARTY_ADDRESS", tableType);
			createPasteTableCLPScript(dropDate, newDate, central_No, "PARTY_EMAIL", tableType);
//			createPasteTableCLPScript(dropDate, newDate, central_No, "PARTY_NATIONALITY", tableType);
			createPasteTableCLPScript(dropDate, newDate, central_No, "PARTY_PARTY_REL", tableType);
			createPasteTableCLPScript(dropDate, newDate, central_No, "PARTY_PHONE", tableType);

			// 執行Script
			runCLPScript(central_No, "RENEW", "ACCOUNT", tableType);
			runCLPScript(central_No, "RENEW", "ACCOUNT_PROPERTY", tableType);
			runCLPScript(central_No, "RENEW", "BALANCE", tableType);
			runCLPScript(central_No, "RENEW", "LOAN", tableType);
			runCLPScript(central_No, "RENEW", "LOAN_COLLATERAL", tableType);
			runCLPScript(central_No, "RENEW", "LOAN_DETAIL", tableType);
			runCLPScript(central_No, "RENEW", "LOAN_GUARANTOR", tableType);
			runCLPScript(central_No, "RENEW", "LOAN_MASTER", tableType);
			runCLPScript(central_No, "RENEW", "PARTY", tableType);
			runCLPScript(central_No, "RENEW", "PARTY_ACCOUNT_REL", tableType);
			runCLPScript(central_No, "RENEW", "PARTY_ADDRESS", tableType);
			runCLPScript(central_No, "RENEW", "PARTY_EMAIL", tableType);
//			runCLPScript(central_No, "RENEW", "PARTY_NATIONALITY", tableType);
			runCLPScript(central_No, "RENEW", "PARTY_PARTY_REL", tableType);
			runCLPScript(central_No, "RENEW", "PARTY_PHONE", tableType);
				
		} catch (Exception ex) {
			ex.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	// create產生新一代Table指令(Script)
	private static void createCopyTableCLPScript(Date oldDate, Date newDate, String central_No, String runTableName, String tableType) throws Exception {
		
		System.out.println("建立  新一代Table Script - " + central_No + " - " + runTableName + " - " + tableType + " Start");
		
		ETL_Bean_CLP_Script copyBean = new ETL_Bean_CLP_Script();
		try {
			// CLPScript的檔案模組路徑位置
			copyBean.setModelFilePath(ETL_Profile.ETL_E_CLP_MODEL_SCRIPT_FILE_PATH + "modelCopy_" + runTableName + ".sql");
			// 產出CLPScript的檔案路徑位置
			copyBean.setDestinationFilePath(ETL_Profile.ETL_E_CLP_RUN_SCRIPT_FILE_PATH + "/" + central_No + "/" + "GENERATE_" + runTableName + "_" + tableType + ".sql");

			HashMap<Integer, String> copyMap = new HashMap<Integer, String>();
			// 放置檔案模組路的字串問號所代表內容 key為問號的順序
			copyMap.put(1, central_No);
			copyMap.put(2, tableType);
			copyMap.put(3, ETL_Tool_StringX.toUtilDateStr(oldDate, "yyyy-MM-dd"));
			copyMap.put(4, tableType);
			copyMap.put(5, tableType);
			copyMap.put(6, ETL_Tool_StringX.toUtilDateStr(newDate, "yyyy-MM-dd"));
			
			copyBean.setMap(copyMap);
			
		} catch (Exception e) {
			e.printStackTrace();
//			return false;
			throw new Exception(e.getMessage());
		}

		if (createCLPScript(copyBean)) {
			System.out.println("建立   新一代Table Script - " + central_No + " - " + runTableName + " - " + tableType + " End");
//			return true;
		} else {
			System.out.println("建立   新一代Table Script - " + central_No + " - " + runTableName + " - " + tableType + " 失敗!");
//			return false;
			throw new Exception("建立   新一代Table Script - " + central_No + " - " + runTableName + " - " + tableType + " 失敗!");
		}
	}
	
	// create 五代Table除舊佈新指令(Script)
	private static boolean createPasteTableCLPScript(Date dropDate, Date newDate, String central_No, String runTableName, String tableType) {
		
		System.out.println("建立  汰換5代Table Script - " + central_No + " - " + runTableName + " - " + tableType + " Start");
		
		ETL_Bean_CLP_Script pasteBean = new ETL_Bean_CLP_Script();
		try {
			// CLPScript的檔案模組路徑位置
			pasteBean.setModelFilePath(ETL_Profile.ETL_E_CLP_MODEL_SCRIPT_FILE_PATH + "modelPaste_" + runTableName + ".sql");
			// 產出CLPScript的檔案路徑位置
			pasteBean.setDestinationFilePath(ETL_Profile.ETL_E_CLP_RUN_SCRIPT_FILE_PATH + "/" + central_No + "/" + "RENEW_" + runTableName + "_" + tableType + ".sql");
			
			HashMap<Integer, String> pasteMap = new HashMap<Integer, String>();
			// 放置檔案模組路的字串問號所代表內容 key為問號的順序
			pasteMap.put(1, central_No);
			pasteMap.put(2, ETL_Tool_StringX.toUtilDateStr(newDate, "yyyyMMdd"));
			pasteMap.put(3, ETL_Tool_StringX.toUtilDateStr(newDate, "yyyy-MM-dd"));
			pasteMap.put(4, ETL_Tool_StringX.toUtilDateStr(newDate, "yyyy-MM-dd"));
			pasteMap.put(5, tableType);
			if (dropDate == null) {
				pasteMap.put(6, null);
			} else {
				pasteMap.put(6, ETL_Tool_StringX.toUtilDateStr(dropDate, "yyyyMMdd"));
			}
			
			pasteBean.setMap(pasteMap);
			
		} catch (Exception e) {
			System.out.println("建立  汰換5代Table Script - " + central_No + " - " + runTableName + " - " + tableType + " 發生錯誤!");
			e.printStackTrace();
			return false;
		}
		
		if (createCLPScript(pasteBean)) {
			System.out.println("建立  汰換5代Table Script - " + central_No + " - " + runTableName + " - " + tableType + " End");
			return true;
		} else {
			System.out.println("建立  汰換5代Table Script - " + central_No + " - " + runTableName + " - " + tableType + " 失敗!");
			return false;
		}
	}
	
	// 產生指令Script
	private static boolean createCLPScript(ETL_Bean_CLP_Script bean) {

		// 抓取檔案model
		File modelFile = new File(bean.getModelFilePath());
		File destinationFile = new File(bean.getDestinationFilePath());

		InputStreamReader isr;
		try {

			if (!destinationFile.getParentFile().isDirectory()) {
				destinationFile.getParentFile().mkdirs();
			}
			
			if (!destinationFile.exists()) {
				destinationFile.createNewFile();
			}

			if (!modelFile.exists()) {
				System.out.println("來源model錯誤");
				return false;
			}

			isr = new InputStreamReader(new FileInputStream(modelFile), "UTF-8");

			BufferedReader br = new BufferedReader(isr);

			String line;
			StringBuffer sb = new StringBuffer();
			
			// 問號數量初始直
			int count = 1;
			int lineCount = 1;
			while ((line = br.readLine()) != null) {
				
				// 為Paste特製
				if (lineCount == 4 && count == 6 && bean.getMap().get(count) == null) {
					continue;
				}
				
				// 如果此行有 ? 符號 且 有 declare 放入 oldDate
				if (line.contains("?")) {
					
					// 計算此行問號數量
					int charCount = line.length() - line.replaceAll("\\?", "").length();

					// System.out.println("charCount:" + charCount);
					
					// 當此行問號數量為2以上
					if (charCount > 1) {

						// 將問號作為分隔點切割成陣列
						String[] strArr = line.split("\\?");
						
						// 清空此行 ps 因為已經將其資料存放在strArr了
						line = "";

						for (int i = 0; i < strArr.length; i++) {
							line = line + strArr[i];
							
							// 最後一次不替換
							if (i < strArr.length - 1) {
								line = line + bean.getMap().get(count);
								count++;
							}
						}

					} else {
						// 當問號數量為1 直接替換
						line = line.replace("?", bean.getMap().get(count));
						count++;
					}

				}
				sb.append(line);
				
				lineCount++;
			}
			
		
			// 換行
			sb.replace(0, sb.length(), ((sb.toString()).replaceAll(";", ";\n")));
			br.close();

			BufferedWriter bwr = new BufferedWriter(new FileWriter(destinationFile));

			bwr.write(sb.toString());

			bwr.flush();

			bwr.close();

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	// 執行完成Script
	private static void runCLPScript(String central_No, String runType, String runTableName,String tableType) throws Exception {
		String scriptName = runType + "_" + runTableName + "_" + tableType + ".sql";
		
		System.out.println("執行 " + scriptName + " Start " + new Date());
		
		try {
			// for test
			System.out.println("db2cmd -c -i db2 -tvmf " + ETL_Profile.ETL_E_CLP_RUN_SCRIPT_FILE_PATH + central_No + "/" + scriptName);
			runProcess("db2cmd -c -i db2 -tvmf " + ETL_Profile.ETL_E_CLP_RUN_SCRIPT_FILE_PATH + central_No + "/" + scriptName);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		System.out.println("執行 " + scriptName + " End" + new Date());
	}
	
	// 執行指令(Script)
	private static void runProcess(String command) throws Exception {
		Process process = null;
		
		try {
			Runtime rt = Runtime.getRuntime();
			process = rt.exec(command);
			
			// 程序字串輸出
			String line = null;
			BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), "BIG5"));
			while ((line = reader.readLine()) != null) {
				System.out.println(line);
			}
			reader.close();
						
			// 等待剛剛執行的命令的結束
			if (process.waitFor() == 0) {
//				System.out.println("#################  process over:" + process.waitFor());	
				System.out.println("執行 " + command + " 正常結束");
			} else {
//				System.out.println("process not over:" + process.waitFor());
				System.out.println("執行 " + command + " 異常！");
			}
//				Thread.sleep(3000);
				
//			}
			
//			System.out.println("runProcess end");
			
		} catch (Exception ex) {
			process.destroy();
			ex.printStackTrace();
			throw new Exception("執行command : " + command + "  失敗！");
		}
		
	}
	
	// 寫入5代Table 記錄檔
	private static boolean writeStartGenerationStatus(Date record_date, String central_no) {
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.write_Start_Generation_Status(?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setDate(2, new java.sql.Date(record_date.getTime()));
			cstmt.setString(3, central_no);
			cstmt.registerOutParameter(4, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(4);
	            System.out.println("####writeNewGenerationStatus - Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            return false;
			}
			
			return true;
		} catch (Exception ex) {
			ex.printStackTrace();
			return false;
		}
	}
	
	// 更新5代Table 記錄檔  prepare status
	public static boolean updateNewGenerationPrepareStatus(Date record_date, String central_no, String prepareStatus, String discription) {
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.update_New_Generation_Prepare_Status(?,?,?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setDate(2, new java.sql.Date(record_date.getTime()));
			cstmt.setString(3, central_no);
			cstmt.setString(4, prepareStatus);
			cstmt.setString(5, discription);
			cstmt.registerOutParameter(6, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(6);
	            System.out.println("####writeNewGenerationStatus - Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            return false;
			}
			
			return true;
		} catch (Exception ex) {
			ex.printStackTrace();
			return false;
		}
	}
	
	private static boolean detachTablePartiton(String central_no, Date recordDate) {
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".new5G_Service.detach_Table_Partiton(?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection(central_no);
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setDate(2, new java.sql.Date(recordDate.getTime()));
			cstmt.registerOutParameter(3, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(3);
	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
//	            throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            return false;
			}
			
			System.out.println("detach " + central_no +  " partition " + new SimpleDateFormat("yyyyMMdd").format(recordDate) +  " 成功!!");
			return true;
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return false;
		}
	}
	
	// 查詢是否寫過5代記錄檔
	private static boolean hasRunGeneration(String central_no, Date record_date) throws Exception {
		
		String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.hasRunGeneration(?,?,?,?,?)}";
		
		Connection con = ConnectionHelper.getDB2Connection();
		CallableStatement cstmt = con.prepareCall(sql);
		
		cstmt.registerOutParameter(1, Types.INTEGER);
		cstmt.setString(2, central_no);
		cstmt.setDate(3, new java.sql.Date(record_date.getTime()));
		cstmt.registerOutParameter(4, Types.INTEGER);
		cstmt.registerOutParameter(5, Types.VARCHAR);
		
		cstmt.execute();
		
		int returnCode = cstmt.getInt(1);
		
		// 有錯誤釋出錯誤訊息   不往下繼續進行
		if (returnCode != 0) {
			String errorMessage = cstmt.getString(5);
            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
            throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
		}
		
		int hasRunCount = cstmt.getInt(4);
		
		if (hasRunCount > 0) {
			// 有過紀錄回傳true
			return true;
		} else {
			// 沒有記錄回傳false
			return false;
		}
			
	}
	
	// Transaction超過一定門檻時執行reorg
	public static void reorgTransaction(String central_no, Date oldRecordDate) {
		
		try {
			
			System.out.println("####ETL_C_New5G - reorgTransaction 單位:" + central_no + " Start  " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Load.reorgTransaction(?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection(central_no);
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setDate(2, new java.sql.Date(oldRecordDate.getTime()));
			cstmt.registerOutParameter(3, Types.INTEGER);
			cstmt.registerOutParameter(4, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(4);
	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
//		            throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			}
			
			int isExecute = cstmt.getInt(3);
			
			System.out.println("isExecute = " + isExecute);
			if (isExecute > 0) {
				System.out.println("單位: " + central_no + "  Transaction  已執行Reorg!!");
			} else {
				System.out.println("單位: " + central_no + "  Transaction  Reorg未執行。");
			}
			
			System.out.println("####ETL_C_New5G - reorgTransaction 單位:" + central_no + " End  " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	public static void main(String[] argv) throws Exception {
		
//		System.out.println("測試開始！");
//		
//		detachTablePartiton("952", new SimpleDateFormat("yyyyMMdd").parse("20180515"));
//		
//		System.out.println("測試結束！");
		
//		renew5GTable(ETL_Tool_StringX.toUtilDate("20180111"), ETL_Tool_StringX.toUtilDate("20180430"), "605", "TEMP");
		
//		System.out.println("##########################################");
//		
//		String central_No = "018";
//
//		// FOR TEST
//		Date oldDate = ETL_Tool_StringX.toUtilDate("20180111");
//		Date newDate = ETL_Tool_StringX.toUtilDate("20180112");
//
//		boolean isSucess = createCopyTableCLPScript(oldDate, newDate, central_No, "PARTY_PHONE", "TEMP");
//		isSucess = createCopyTableCLPScript(oldDate, newDate, central_No, "PARTY_PARTY_REL", "RERUN");
//
//		if (isSucess) {
//			
//			runCLPScript(central_No, "GENERATE", "PARTY_PHONE");
//			runCLPScript(central_No, "GENERATE", "PARTY_PARTY_REL");
//			
//		} else {
//			System.out.println("create Copy CLPScript ERROR!");
//		}
//
//		// FOR TEST
//		Date dropDate = ETL_Tool_StringX.toUtilDate("20180107");
//		
//		isSucess = createPasteTableCLPScript(newDate, dropDate, central_No, "PARTY_PHONE", "TEMP");
//		isSucess = createPasteTableCLPScript(newDate, dropDate, central_No, "PARTY_PARTY_REL", "RERUN");
//
//		if (isSucess) {
//			
//			runCLPScript(central_No, "RENEW", "PARTY_PHONE");
//			runCLPScript(central_No, "RENEW", "PARTY_PARTY_REL");
//			
//		} else {
//			System.out.println("create Paste CLPScript ERROR!");
//		}
//		
//		// FOR TEST
//		boolean isSucess;
//		Date dropDate = ETL_Tool_StringX.toUtilDate("20180101");
//		Date newDate = ETL_Tool_StringX.toUtilDate("20180412");
//		
//		isSucess = createPasteTableCLPScript(dropDate, newDate, "018", "BALANCE", "TEMP");
////		isSucess = createPasteTableCLPScript(dropDate, newDate, "018", "PARTY_PARTY_REL", "RERUN");
//
//		if (isSucess) {
//			
//			runCLPScript("018", "RENEW", "BALANCE","TEMP");
////			runCLPScript(central_No, "RENEW", "PARTY_PARTY_REL");
//			
//		} else {
//			System.out.println("create Paste CLPScript ERROR!");
//		}

//		Date dropDate = new SimpleDateFormat("yyyyMMdd").parse("20180410");
//		Date newDate = new SimpleDateFormat("yyyyMMdd").parse("20180427");
//		Date oldDate = new SimpleDateFormat("yyyyMMdd").parse("20180426");
//
//		String central_No = "910";
//		String tableType = "TEMP";
//
//		System.out.println("測試開始");
//
//		// 產生新一代Table Script
//		createCopyTableCLPScript(oldDate, newDate, central_No, "ACCOUNT", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "ACCOUNT_PROPERTY", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "BALANCE", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "LOAN", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "LOAN_COLLATERAL", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "LOAN_DETAIL", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "LOAN_GUARANTOR", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "LOAN_MASTER", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "PARTY", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "PARTY_ACCOUNT_REL", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "PARTY_ADDRESS", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "PARTY_EMAIL", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "PARTY_NATIONALITY", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "PARTY_PARTY_REL", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "PARTY_PHONE", tableType);
//////
//////		// 執行Script
//		runCLPScript(central_No, "GENERATE", "ACCOUNT", tableType);
//		runCLPScript(central_No, "GENERATE", "ACCOUNT_PROPERTY", tableType);
//		runCLPScript(central_No, "GENERATE", "BALANCE", tableType);
//		runCLPScript(central_No, "GENERATE", "LOAN", tableType);
//		runCLPScript(central_No, "GENERATE", "LOAN_COLLATERAL", tableType);
//		runCLPScript(central_No, "GENERATE", "LOAN_DETAIL", tableType);
//		runCLPScript(central_No, "GENERATE", "LOAN_GUARANTOR", tableType);
//		runCLPScript(central_No, "GENERATE", "LOAN_MASTER", tableType);
//		runCLPScript(central_No, "GENERATE", "PARTY", tableType);
//		runCLPScript(central_No, "GENERATE", "PARTY_ACCOUNT_REL", tableType);
//		runCLPScript(central_No, "GENERATE", "PARTY_ADDRESS", tableType);
//		runCLPScript(central_No, "GENERATE", "PARTY_EMAIL", tableType);
//		runCLPScript(central_No, "GENERATE", "PARTY_NATIONALITY", tableType);
//		runCLPScript(central_No, "GENERATE", "PARTY_PARTY_REL", tableType);
//		runCLPScript(central_No, "GENERATE", "PARTY_PHONE", tableType);
//
//		// 產生新一代Table Script
//		createPasteTableCLPScript(dropDate, newDate, central_No, "ACCOUNT", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "ACCOUNT_PROPERTY", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "BALANCE", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "LOAN", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "LOAN_COLLATERAL", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "LOAN_DETAIL", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "LOAN_GUARANTOR", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "LOAN_MASTER", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "PARTY", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "PARTY_ACCOUNT_REL", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "PARTY_ADDRESS", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "PARTY_EMAIL", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "PARTY_NATIONALITY", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "PARTY_PARTY_REL", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "PARTY_PHONE", tableType);
//
//		// 執行Script
//		runCLPScript(central_No, "RENEW", "ACCOUNT", tableType);
//		runCLPScript(central_No, "RENEW", "ACCOUNT_PROPERTY", tableType);
//		runCLPScript(central_No, "RENEW", "BALANCE", tableType);
//		runCLPScript(central_No, "RENEW", "LOAN", tableType);
//		runCLPScript(central_No, "RENEW", "LOAN_COLLATERAL", tableType);
//		runCLPScript(central_No, "RENEW", "LOAN_DETAIL", tableType);
//		runCLPScript(central_No, "RENEW", "LOAN_GUARANTOR", tableType);
//		runCLPScript(central_No, "RENEW", "LOAN_MASTER", tableType);
//		runCLPScript(central_No, "RENEW", "PARTY", tableType);
//		runCLPScript(central_No, "RENEW", "PARTY_ACCOUNT_REL", tableType);
//		runCLPScript(central_No, "RENEW", "PARTY_ADDRESS", tableType);
//		runCLPScript(central_No, "RENEW", "PARTY_EMAIL", tableType);
//		runCLPScript(central_No, "RENEW", "PARTY_NATIONALITY", tableType);
//		runCLPScript(central_No, "RENEW", "PARTY_PARTY_REL", tableType);
//		runCLPScript(central_No, "RENEW", "PARTY_PHONE", tableType);
//
//		dropDate = new SimpleDateFormat("yyyyMMdd").parse("20180410");
//		newDate = new SimpleDateFormat("yyyyMMdd").parse("20180428");
//		oldDate = new SimpleDateFormat("yyyyMMdd").parse("20180427");
////
//		tableType = "RERUN";
////
//		System.out.println("測試開始2");
////
////		// 產生新一代Table Script
//		createCopyTableCLPScript(oldDate, newDate, central_No, "ACCOUNT", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "ACCOUNT_PROPERTY", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "BALANCE", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "LOAN", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "LOAN_COLLATERAL", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "LOAN_DETAIL", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "LOAN_GUARANTOR", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "LOAN_MASTER", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "PARTY", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "PARTY_ACCOUNT_REL", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "PARTY_ADDRESS", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "PARTY_EMAIL", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "PARTY_NATIONALITY", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "PARTY_PARTY_REL", tableType);
//		createCopyTableCLPScript(oldDate, newDate, central_No, "PARTY_PHONE", tableType);
//
//		// 執行Script
//		runCLPScript(central_No, "GENERATE", "ACCOUNT", tableType);
//		runCLPScript(central_No, "GENERATE", "ACCOUNT_PROPERTY", tableType);
//		runCLPScript(central_No, "GENERATE", "BALANCE", tableType);
//		runCLPScript(central_No, "GENERATE", "LOAN", tableType);
//		runCLPScript(central_No, "GENERATE", "LOAN_COLLATERAL", tableType);
//		runCLPScript(central_No, "GENERATE", "LOAN_DETAIL", tableType);
//		runCLPScript(central_No, "GENERATE", "LOAN_GUARANTOR", tableType);
//		runCLPScript(central_No, "GENERATE", "LOAN_MASTER", tableType);
//		runCLPScript(central_No, "GENERATE", "PARTY", tableType);
//		runCLPScript(central_No, "GENERATE", "PARTY_ACCOUNT_REL", tableType);
//		runCLPScript(central_No, "GENERATE", "PARTY_ADDRESS", tableType);
//		runCLPScript(central_No, "GENERATE", "PARTY_EMAIL", tableType);
//		runCLPScript(central_No, "GENERATE", "PARTY_NATIONALITY", tableType);
//		runCLPScript(central_No, "GENERATE", "PARTY_PARTY_REL", tableType);
//		runCLPScript(central_No, "GENERATE", "PARTY_PHONE", tableType);
////
//		// 產生新一代Table Script
//		createPasteTableCLPScript(dropDate, newDate, central_No, "ACCOUNT", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "ACCOUNT_PROPERTY", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "BALANCE", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "LOAN", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "LOAN_COLLATERAL", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "LOAN_DETAIL", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "LOAN_GUARANTOR", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "LOAN_MASTER", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "PARTY", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "PARTY_ACCOUNT_REL", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "PARTY_ADDRESS", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "PARTY_EMAIL", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "PARTY_NATIONALITY", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "PARTY_PARTY_REL", tableType);
//		createPasteTableCLPScript(dropDate, newDate, central_No, "PARTY_PHONE", tableType);
//
//		// 執行Script
//		runCLPScript(central_No, "RENEW", "ACCOUNT", tableType);
//		runCLPScript(central_No, "RENEW", "ACCOUNT_PROPERTY", tableType);
//		runCLPScript(central_No, "RENEW", "BALANCE", tableType);
//		runCLPScript(central_No, "RENEW", "LOAN", tableType);
//		runCLPScript(central_No, "RENEW", "LOAN_COLLATERAL", tableType);
//		runCLPScript(central_No, "RENEW", "LOAN_DETAIL", tableType);
//		runCLPScript(central_No, "RENEW", "LOAN_GUARANTOR", tableType);
//		runCLPScript(central_No, "RENEW", "LOAN_MASTER", tableType);
//		runCLPScript(central_No, "RENEW", "PARTY", tableType);
//		runCLPScript(central_No, "RENEW", "PARTY_ACCOUNT_REL", tableType);
//		runCLPScript(central_No, "RENEW", "PARTY_ADDRESS", tableType);
//		runCLPScript(central_No, "RENEW", "PARTY_EMAIL", tableType);
//		runCLPScript(central_No, "RENEW", "PARTY_NATIONALITY", tableType);
//		runCLPScript(central_No, "RENEW", "PARTY_PARTY_REL", tableType);
//		runCLPScript(central_No, "RENEW", "PARTY_PHONE", tableType);
	}

}
