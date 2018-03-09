package DB;

import java.util.ArrayList;
import java.util.List;

import Bean.ETL_Bean_ErrorLog_Data;
import Profile.ETL_Profile;

public class ETL_P_ErrorLog_Writer {
	// Error Log寫入工具
	
	// Error寫入域值
	private int stageLimit = ETL_Profile.ErrorLog_Stage; 
//	private int stageLimit = 10000; // for test
	
	// Error Log計數
	private int errorLogCount = 0;
	
	// Error Log儲存List
	private List<ETL_Bean_ErrorLog_Data> errorLogList = new ArrayList<ETL_Bean_ErrorLog_Data>();
	
	public void addErrLog(ETL_Bean_ErrorLog_Data errorLog) throws Exception {
		if (errorLog == null) {
			throw new Exception("無接收到ErrorLog實體!");
//			System.out.println("無接收到實際Log");
		}
		
		this.errorLogList.add(errorLog);
		this.errorLogCount++;
		
		// 若超過域值  先行寫入DB
		if (this.errorLogCount == stageLimit) {
			insert_ErrorLog_To_DB();
			this.errorLogCount = 0; // 計數歸0
			this.errorLogList.clear(); // 清空list
		}
	}
	
	// 將現有Error_Log 觸發命令寫入DB
	public void insert_Error_Log() throws Exception {
		insert_ErrorLog_To_DB();
	}
	
	// Error Log寫入
	private void insert_ErrorLog_To_DB() throws Exception {
		if (this.errorLogList == null || this.errorLogList.size() == 0) {
			System.out.println("ETL_P_ErrorLog_Writer - insert_ErrorLog 無寫入任何資料");
			return;
		}
		
		InsertAdapter insertAdapter = new InsertAdapter(); 
		insertAdapter.setSql("{call SP_INSERT_ERROR_LOGS(?)}"); // 呼叫error_log寫入DB2 - SP
		insertAdapter.setCreateArrayTypesName("A_ERROR_LOG"); // DB2 type - error_log
		insertAdapter.setCreateStructTypeName("T_ERROR_LOG"); // DB2 array type - error_log
		insertAdapter.setTypeArrayLength(ETL_Profile.ErrorLog_Stage);  // 設定上限寫入參數

		Boolean isSuccess = ETL_P_Data_Writer.insertByDefineArrayListObject(this.errorLogList, insertAdapter);
		
		if (isSuccess) {
			System.out.println("insert_ErrorLog 寫入 " + this.errorLogList.size() + " 筆資料!");
		} else {
			throw new Exception("insert_ErrorLog 發生錯誤");
		}
		
		this.errorLogCount = 0; // 計數歸0
		this.errorLogList.clear(); // 清空list
	}
	
}
