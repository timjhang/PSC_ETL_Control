package Bean;

import java.util.Date;

public class ETL_Bean_LogData {

	// ETL_FILE_LOG
	private String BATCH_NO; // 批次編號
	private String CENTRAL_NO; // 報送單位
	private Date RECORD_DATE; // 檔案日期
	private String FILE_TYPE; // 檔名業務別
//	private String FILE_NAME; // 檔案名稱
	private String UPLOAD_NO; // 上傳批號
//	private String STEP_TYPE; // 步驟
//	private Date START_DATETIME; // 執行開始日期時間(Timestamp)
//	private Date END_DATETIME; // 執行結束日期時間(Timestamp)
//	private Integer TOTAL_CNT; // 總筆數
//	private Integer SUCCESS_CNT; // 成功筆數
//	private Integer FAILED_CNT; // 失敗筆數
//	private String SRC_FILE; // 來源檔案
	
	// ETL_Detail_Log
//	private String BATCH_NO ; // 批次編號
//	private String CENTRAL_NO; // 報送單位
//	private Date RECORD_DATE; // 檔案日期
//	private String UPLOAD_NO; // 上傳批號
//	private String STEP_TYPE; // 步驟
	private String PROGRAM_NO; // 程式代號
//	private String EXE_STATUS; // 執行狀態
//	private String EXE_RESULT; // 執行結果
//	private String EXE_RESULT_DESCRIPTION; // 執行結果說明
//	private Date START_DATETIME; // 開始日期時間(Timestamp)
//	private Date END_DATETIME; // 結束日期時間(Timestamp)

	// Error_Log
//	private String BATCH_NO; // 批次編號
//	private String CENTRAL_NO; // 報送單位
//	private Date RECORD_DATE; // 檔案日期
//	private String FILE_TYPE; // 檔名業務別
//	private String FILE_NAME; // 檔案名稱
//	private String UPLOAD_NO; // 上傳批號
//	private String STEP_TYPE; // 步驟
//	private Integer ROW_COUNT; // 行數
//	private String FIELD_NAME; // 欄位中文名稱
//	private String ERROR_DESCRIPTION; // 錯誤描述
//	private String SRC_FILE; // 來源檔案
	
	//前代日期
	private Date BEFORE_ETL_PROCESS_DATE;
	
	
	// clone class方法
	public ETL_Bean_LogData clone() {
		ETL_Bean_LogData newOne = new ETL_Bean_LogData();
		
		newOne.setBATCH_NO(this.BATCH_NO);
		newOne.setCENTRAL_NO(this.CENTRAL_NO);
		newOne.setRECORD_DATE(this.RECORD_DATE);
		newOne.setFILE_TYPE(this.FILE_TYPE);
		newOne.setUPLOAD_NO(this.UPLOAD_NO);
		newOne.setPROGRAM_NO(this.PROGRAM_NO);
		newOne.setBEFORE_ETL_PROCESS_DATE(this.BEFORE_ETL_PROCESS_DATE);
		
		return newOne;
	}
	
	
	public String getBATCH_NO() {
		return BATCH_NO;
	}

	public void setBATCH_NO(String bATCH_NO) {
		BATCH_NO = bATCH_NO;
	}

	public String getCENTRAL_NO() {
		return CENTRAL_NO;
	}

	public void setCENTRAL_NO(String cENTRAL_NO) {
		CENTRAL_NO = cENTRAL_NO;
	}

	public Date getRECORD_DATE() {
		return RECORD_DATE;
	}

	public void setRECORD_DATE(Date rECORD_DATE) {
		RECORD_DATE = rECORD_DATE;
	}

	public String getFILE_TYPE() {
		return FILE_TYPE;
	}

	public void setFILE_TYPE(String fILE_TYPE) {
		FILE_TYPE = fILE_TYPE;
	}

	public String getUPLOAD_NO() {
		return UPLOAD_NO;
	}

	public void setUPLOAD_NO(String uPLOAD_NO) {
		UPLOAD_NO = uPLOAD_NO;
	}

	public String getPROGRAM_NO() {
		return PROGRAM_NO;
	}

	public void setPROGRAM_NO(String pROGRAM_NO) {
		PROGRAM_NO = pROGRAM_NO;
	}

	public Date getBEFORE_ETL_PROCESS_DATE() {
		return BEFORE_ETL_PROCESS_DATE;
	}

	public void setBEFORE_ETL_PROCESS_DATE(Date bEFORE_ETL_PROCESS_DATE) {
		BEFORE_ETL_PROCESS_DATE = bEFORE_ETL_PROCESS_DATE;
	}

}
