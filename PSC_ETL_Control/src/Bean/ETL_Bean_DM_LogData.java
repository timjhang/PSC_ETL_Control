package Bean;

import java.sql.Timestamp;
import java.util.Date;

public class ETL_Bean_DM_LogData {
	private String	batch_no; 				
	private String	central_no; 			
	private Date	record_date; 			
	private String	file_type; 				
	private String	file_name; 				
	private String	upload_no; 				
	private String	step_type; 				
	private Timestamp start_datetime; 		
	private Timestamp end_datetime; 		
	private Integer	total_cnt; 				
	private Integer	success_cnt; 			
	private Integer	failed_cnt; 			
	private String	exe_result; 			
	private String	exe_result_description; 
	private String	src_file;
	
	public String getBatch_no() {
		return batch_no;
	}
	public void setBatch_no(String batch_no) {
		this.batch_no = batch_no;
	}
	public String getCentral_no() {
		return central_no;
	}
	public void setCentral_no(String central_no) {
		this.central_no = central_no;
	}
	public Date getRecord_date() {
		return record_date;
	}
	public void setRecord_date(Date record_date) {
		this.record_date = record_date;
	}
	public String getFile_type() {
		return file_type;
	}
	public void setFile_type(String file_type) {
		this.file_type = file_type;
	}
	public String getFile_name() {
		return file_name;
	}
	public void setFile_name(String file_name) {
		this.file_name = file_name;
	}
	public String getUpload_no() {
		return upload_no;
	}
	public void setUpload_no(String upload_no) {
		this.upload_no = upload_no;
	}
	public String getStep_type() {
		return step_type;
	}
	public void setStep_type(String step_type) {
		this.step_type = step_type;
	}
	public Timestamp getStart_datetime() {
		return start_datetime;
	}
	public void setStart_datetime(Timestamp start_datetime) {
		this.start_datetime = start_datetime;
	}
	public Timestamp getEnd_datetime() {
		return end_datetime;
	}
	public void setEnd_datetime(Timestamp end_datetime) {
		this.end_datetime = end_datetime;
	}
	public Integer getTotal_cnt() {
		return total_cnt;
	}
	public void setTotal_cnt(Integer total_cnt) {
		this.total_cnt = total_cnt;
	}
	public Integer getSuccess_cnt() {
		return success_cnt;
	}
	public void setSuccess_cnt(Integer success_cnt) {
		this.success_cnt = success_cnt;
	}
	public Integer getFailed_cnt() {
		return failed_cnt;
	}
	public void setFailed_cnt(Integer failed_cnt) {
		this.failed_cnt = failed_cnt;
	}
	public String getExe_result() {
		return exe_result;
	}
	public void setExe_result(String exe_result) {
		this.exe_result = exe_result;
	}
	public String getExe_result_description() {
		return exe_result_description;
	}
	public void setExe_result_description(String exe_result_description) {
		this.exe_result_description = exe_result_description;
	}
	public String getSrc_file() {
		return src_file;
	}
	public void setSrc_file(String src_file) {
		this.src_file = src_file;
	} 	
	
	
	// clone class方法
	public ETL_Bean_DM_LogData clone() {
		ETL_Bean_DM_LogData newOne = new ETL_Bean_DM_LogData();
		
		newOne.setBatch_no(this.batch_no);
		newOne.setCentral_no(this.central_no);
		newOne.setEnd_datetime(this.end_datetime);
		newOne.setExe_result(this.exe_result);
		newOne.setExe_result_description(this.exe_result_description);
		newOne.setFailed_cnt(this.failed_cnt);
		newOne.setFile_name(this.file_name);
		newOne.setFile_type(this.file_type);
		newOne.setRecord_date(this.record_date);
		newOne.setSrc_file(this.src_file);
		newOne.setStart_datetime(this.start_datetime);
		newOne.setStep_type(this.step_type);
		newOne.setSuccess_cnt(this.success_cnt);
		newOne.setTotal_cnt(this.total_cnt);
		newOne.setUpload_no(this.upload_no);
	
		return newOne;
	}
	
}
