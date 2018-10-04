package Bean;

import java.util.Date;

import Tool.ETL_Tool_DM_ParseFileName;

public class ETL_Bean_DM_IDMAPPING_LOAD_Data {
	private String old_central_no;//舊隸屬資訊中心代號
	private String old_domain_id;//舊隸屬信用部代號
	private String old_party_id;//舊顧客編號
	private String new_central_no;//新隸屬資訊中心代號
	private String new_domain_id;//新隸屬信用部代號
	private String new_party_id;//新顧客編號
	private Date record_date;//檔案日期
	private String batch_no;//批次編號
	private Date create_date;//創建時間
	private Integer row_count = 0;// 行數
	private String error_mark = "";// 錯誤註記
	
	public ETL_Bean_DM_IDMAPPING_LOAD_Data(ETL_Tool_DM_ParseFileName pfn) {
		this.record_date = pfn.getRecord_date();// 檔案日期
		this.batch_no = pfn.getBatch_no();// 批次編號
	}
	
	public String getOld_central_no() {
		return old_central_no;
	}
	public void setOld_central_no(String old_central_no) {
		this.old_central_no = old_central_no;
	}
	public String getOld_domain_id() {
		return old_domain_id;
	}
	public void setOld_domain_id(String old_domain_id) {
		this.old_domain_id = old_domain_id;
	}
	public String getOld_party_id() {
		return old_party_id;
	}
	public void setOld_party_id(String old_party_id) {
		this.old_party_id = old_party_id;
	}
	public String getNew_central_no() {
		return new_central_no;
	}
	public void setNew_central_no(String new_central_no) {
		this.new_central_no = new_central_no;
	}
	public String getNew_domain_id() {
		return new_domain_id;
	}
	public void setNew_domain_id(String new_domain_id) {
		this.new_domain_id = new_domain_id;
	}
	public String getNew_party_id() {
		return new_party_id;
	}
	public void setNew_party_id(String new_party_id) {
		this.new_party_id = new_party_id;
	}
	public Date getRecord_date() {
		return record_date;
	}
	public void setRecord_date(Date record_date) {
		this.record_date = record_date;
	}
	public String getBatch_no() {
		return batch_no;
	}
	public void setBatch_no(String batch_no) {
		this.batch_no = batch_no;
	}
	public Date getCreate_date() {
		return create_date;
	}
	public void setCreate_date(Date create_date) {
		this.create_date = create_date;
	}

	public Integer getRow_count() {
		return row_count;
	}
	public void setRow_count(Integer row_count) {
		this.row_count = row_count;
	}
	public String getError_mark() {
		return error_mark;
	}
	public void setError_mark(String error_mark) {
		this.error_mark = error_mark;
	}

}
