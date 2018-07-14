package Bean;

import java.util.Date;

public class ETL_Bean_MigrationUnit {
	
	// 中心代碼
	String centralNo;
	// 資料日期
	Date recordDate;
	
	public String getCentralNo() {
		return centralNo;
	}
	
	public void setCentralNo(String centralNo) {
		this.centralNo = centralNo;
	}
	
	public Date getRecordDate() {
		return recordDate;
	}
	
	public void setRecordDate(Date recordDate) {
		this.recordDate = recordDate;
	}

}
