package Bean;

import java.math.BigDecimal;
import java.util.Date;

import Tool.ETL_Tool_ParseFileName;

public class ETL_Bean_PARTY_Data {

	// 報送單位
	private String central_no;
	// 檔案日期
	private Date record_date;
	// 檔名業務別
	private String file_type;
	// 行數
	private Integer row_count;
	// 本會代號
	private String domain_id;
	// 客戶統編
	private String party_number;
	// 異動代號
	private String change_code;
	// 是否為本行客戶
	private String my_customer_flag;
	// 歸屬本/分會代號
	private String branch_code;
	// 顧客類型
	private String entity_type;
	// 客戶子類型
	private String entity_sub_type;
	// 中文名字
	private String party_first_name_1;
	// 中文姓氏
	private String party_last_name_1;
	// 出生年月日/創立日期
	private Date date_of_birth;
	// 亡故日期
	private Date deceased_date;
	// 國籍
	private String nationality_code;
	// 英文名字
	private String party_first_name_2;
	// 英文姓氏
	private String party_last_name_2;
	// 顧客開戶日期
	private Date open_date;
	// 顧客結清日期
	private Date close_date;
	// 性別
	private String gender;
	// 年收入(法人)
	private Long annual_income;
	// 職業/行業
	private String occupation_code;
	// 婚姻狀況
	private String marital_status_code;
	// 服務機構
	private String employer_name;
	// 服務機構統編
	private String employer;
	// 行內員工註記
	private String employee_flag;
	// 出生地
	private String place_of_birth;
	// 是否具多重國籍(自然人)
	private String multiple_nationality_flag;
	// 第二國籍
	private String nationality_code_2;
	// 顧客電子郵件
	private String email_address;
	// 金融卡約定服務
	private String registered_service_atm;
	// 電話約定服務
	private String registered_service_telephone;
	// 傳真約定服務
	private String registered_service_fax;
	// 網銀約定服務
	private String registered_service_internet;
	// 行動銀行約定服務
	private String registered_service_mobile;
	// 是否得發行無記名股票 (法人)
	private String bearer_stock_flag;
	// 無記名股票 (法人)資訊說明
	private String bearer_stock_description;
	// 外國人士居留或交易目的
	private String foreign_transaction_purpose;
	// 顧客AUM餘額
	private BigDecimal total_asset;
	// 信託客戶AUM餘額
	private BigDecimal trust_total_asset;
	// 錯誤註記
	private String error_mark = ""; // 預設無錯誤
	// 上傳批號(測試用)
	private String upload_no;
	
	// Constructor
	public ETL_Bean_PARTY_Data(ETL_Tool_ParseFileName pfn) {
		this.central_no = pfn.getCentral_No();
		this.record_date = pfn.getRecord_Date();
		this.file_type = pfn.getFile_Type();
		this.upload_no = pfn.getUpload_no();
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

	public Integer getRow_count() {
		return row_count;
	}

	public void setRow_count(Integer row_count) {
		this.row_count = row_count;
	}

	public String getDomain_id() {
		return domain_id;
	}

	public void setDomain_id(String domain_id) {
		this.domain_id = domain_id;
	}

	public String getParty_number() {
		return party_number;
	}

	public void setParty_number(String party_number) {
		this.party_number = party_number;
	}

	public String getChange_code() {
		return change_code;
	}

	public void setChange_code(String change_code) {
		this.change_code = change_code;
	}

	public String getMy_customer_flag() {
		return my_customer_flag;
	}

	public void setMy_customer_flag(String my_customer_flag) {
		this.my_customer_flag = my_customer_flag;
	}

	public String getBranch_code() {
		return branch_code;
	}

	public void setBranch_code(String branch_code) {
		this.branch_code = branch_code;
	}

	public String getEntity_type() {
		return entity_type;
	}

	public void setEntity_type(String entity_type) {
		this.entity_type = entity_type;
	}

	public String getEntity_sub_type() {
		return entity_sub_type;
	}

	public void setEntity_sub_type(String entity_sub_type) {
		this.entity_sub_type = entity_sub_type;
	}

	public String getParty_first_name_1() {
		return party_first_name_1;
	}

	public void setParty_first_name_1(String party_first_name_1) {
		this.party_first_name_1 = party_first_name_1;
	}

	public String getParty_last_name_1() {
		return party_last_name_1;
	}

	public void setParty_last_name_1(String party_last_name_1) {
		this.party_last_name_1 = party_last_name_1;
	}

	public Date getDate_of_birth() {
		return date_of_birth;
	}

	public void setDate_of_birth(Date date_of_birth) {
		this.date_of_birth = date_of_birth;
	}

	public Date getDeceased_date() {
		return deceased_date;
	}

	public void setDeceased_date(Date deceased_date) {
		this.deceased_date = deceased_date;
	}

	public String getNationality_code() {
		return nationality_code;
	}

	public void setNationality_code(String nationality_code) {
		this.nationality_code = nationality_code;
	}

	public String getParty_first_name_2() {
		return party_first_name_2;
	}

	public void setParty_first_name_2(String party_first_name_2) {
		this.party_first_name_2 = party_first_name_2;
	}

	public String getParty_last_name_2() {
		return party_last_name_2;
	}

	public void setParty_last_name_2(String party_last_name_2) {
		this.party_last_name_2 = party_last_name_2;
	}

	public Date getOpen_date() {
		return open_date;
	}

	public void setOpen_date(Date open_date) {
		this.open_date = open_date;
	}

	public Date getClose_date() {
		return close_date;
	}

	public void setClose_date(Date close_date) {
		this.close_date = close_date;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public Long getAnnual_income() {
		return annual_income;
	}

	public void setAnnual_income(Long annual_income) {
		this.annual_income = annual_income;
	}

	public String getOccupation_code() {
		return occupation_code;
	}

	public void setOccupation_code(String occupation_code) {
		this.occupation_code = occupation_code;
	}

	public String getMarital_status_code() {
		return marital_status_code;
	}

	public void setMarital_status_code(String marital_status_code) {
		this.marital_status_code = marital_status_code;
	}

	public String getEmployer_name() {
		return employer_name;
	}

	public void setEmployer_name(String employer_name) {
		this.employer_name = employer_name;
	}

	public String getEmployer() {
		return employer;
	}

	public void setEmployer(String employer) {
		this.employer = employer;
	}

	public String getEmployee_flag() {
		return employee_flag;
	}

	public void setEmployee_flag(String employee_flag) {
		this.employee_flag = employee_flag;
	}

	public String getPlace_of_birth() {
		return place_of_birth;
	}

	public void setPlace_of_birth(String place_of_birth) {
		this.place_of_birth = place_of_birth;
	}

	public String getMultiple_nationality_flag() {
		return multiple_nationality_flag;
	}

	public void setMultiple_nationality_flag(String multiple_nationality_flag) {
		this.multiple_nationality_flag = multiple_nationality_flag;
	}

	public String getNationality_code_2() {
		return nationality_code_2;
	}

	public void setNationality_code_2(String nationality_code_2) {
		this.nationality_code_2 = nationality_code_2;
	}

	public String getEmail_address() {
		return email_address;
	}

	public void setEmail_address(String email_address) {
		this.email_address = email_address;
	}

	public String getRegistered_service_atm() {
		return registered_service_atm;
	}

	public void setRegistered_service_atm(String registered_service_atm) {
		this.registered_service_atm = registered_service_atm;
	}

	public String getRegistered_service_telephone() {
		return registered_service_telephone;
	}

	public void setRegistered_service_telephone(String registered_service_telephone) {
		this.registered_service_telephone = registered_service_telephone;
	}

	public String getRegistered_service_fax() {
		return registered_service_fax;
	}

	public void setRegistered_service_fax(String registered_service_fax) {
		this.registered_service_fax = registered_service_fax;
	}

	public String getRegistered_service_internet() {
		return registered_service_internet;
	}

	public void setRegistered_service_internet(String registered_service_internet) {
		this.registered_service_internet = registered_service_internet;
	}

	public String getRegistered_service_mobile() {
		return registered_service_mobile;
	}

	public void setRegistered_service_mobile(String registered_service_mobile) {
		this.registered_service_mobile = registered_service_mobile;
	}

	public String getBearer_stock_flag() {
		return bearer_stock_flag;
	}

	public void setBearer_stock_flag(String bearer_stock_flag) {
		this.bearer_stock_flag = bearer_stock_flag;
	}

	public String getBearer_stock_description() {
		return bearer_stock_description;
	}

	public void setBearer_stock_description(String bearer_stock_description) {
		this.bearer_stock_description = bearer_stock_description;
	}

	public String getForeign_transaction_purpose() {
		return foreign_transaction_purpose;
	}

	public void setForeign_transaction_purpose(String foreign_transaction_purpose) {
		this.foreign_transaction_purpose = foreign_transaction_purpose;
	}

	public BigDecimal getTotal_asset() {
		return total_asset;
	}

	public void setTotal_asset(BigDecimal total_asset) {
		this.total_asset = total_asset;
	}

	public BigDecimal getTrust_total_asset() {
		return trust_total_asset;
	}

	public void setTrust_total_asset(BigDecimal trust_total_asset) {
		this.trust_total_asset = trust_total_asset;
	}

	public String getError_mark() {
		return error_mark;
	}

	public void setError_mark(String error_mark) {
		this.error_mark = error_mark;
	}

	public String getUpload_no() {
		return upload_no;
	}

	public void setUpload_no(String upload_no) {
		this.upload_no = upload_no;
	}
	
}
