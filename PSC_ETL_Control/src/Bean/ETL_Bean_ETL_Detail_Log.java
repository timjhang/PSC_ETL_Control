package Bean;

import java.sql.Timestamp;
import java.util.Date;

public class ETL_Bean_ETL_Detail_Log {
	// ETL EDT Detail Log Data
	
	private String batch_no; // 批次編號
	private String central_no;	// 報送單位
	private Date record_date; // 檔案日期
	private String upload_no; // 上傳批號
	private String step_type; // 步驟
	private String program_no; // 程式代號
	private String exe_status; // 執行狀態
	private String exe_result; // 執行結果
	private Timestamp exe_result_description; // 執行結果說明
	private Timestamp start_datetime; // 開始日期時間
	private String end_datetime; // 結束日期時間
	
}
