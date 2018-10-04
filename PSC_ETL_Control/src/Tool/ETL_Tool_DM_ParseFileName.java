package Tool;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ETL_Tool_DM_ParseFileName {

	private String central_no;//報送單位
	
	private String fileName;// 檔案名
	
	private String file_name;// 檔案名
	
	private String batch_no;// 批次編號
	
	private Date record_date;// 檔案日期
	
	private String record_date_str;// 檔案日期文字
	
	/*
	 * 檔名格式AA_BBB_CCCC_yyyyMMdd.txt
	 */
	public ETL_Tool_DM_ParseFileName (String fileName) throws Exception {
		// 檔名非空檢核
		if (ETL_Tool_FormatCheck.isEmpty(fileName)) {
			throw new Exception("ETL_Tool_DM_ParseFileName 建立檔名有誤!!\n無檔名");
		}
		// 檔名"_"比較檢核
		if (fileName.split("\\_").length != 4) {
			throw new Exception("ETL_Tool_DM_ParseFileName 建立檔名有誤!!\n檔名格式\"_\"數目錯誤");
		}
		// 副檔名必要檢核
		if (fileName.split("\\.").length != 2) {
			throw new Exception("ETL_Tool_DM_ParseFileName 建立檔名有誤!!\n檔名副檔名格式錯誤");
		}
		
		// 副檔名必要檢核
		if (!"TXT".equals(fileName.split("\\.")[1].toUpperCase() )) {
			throw new Exception("ETL_Tool_DM_ParseFileName 建立檔名有誤!!\n 該副檔名不為TXT");
		}
		
		// 起頭為TR
		if (!"TR".equals(fileName.split("\\_")[0])) {
			System.out.println(fileName.split("\\_")[0].trim());
			throw new Exception("ETL_Tool_DM_ParseFileName 建立檔名有誤!!\n 該檔名不為起頭不為TR");
		}
		
		
		String mainName = fileName.split("\\.")[0];
		String source = mainName.substring(mainName.length() - 8, mainName.length());
		
		this.fileName = fileName;
		this.file_name = mainName.split("\\_")[2].trim(); // 寫入處裡檔名
		this.central_no = mainName.split("\\_")[1].trim(); // 寫入報送單位
		this.record_date_str = source; // 寫入檔案日期文字(yyyyMMdd)
		this.record_date = new SimpleDateFormat("yyyyMMdd").parse(source); // 寫入檔案日期
		
		if(!"600".equals(central_no)&&!"952".equals(central_no)) {
			throw new Exception("單位錯誤");
		}
		
		// 日期檢核
		if (record_date == null) {
			throw new Exception("ETL_Tool_DM_ParseFileName 建立檔名有誤!!\n檔名日期格式錯誤");
		}
	
		if(!"BRANCHMAPPING".equals(file_name)&&!"ACCTMAPPING".equals(file_name)&&!"IDMAPPING".equals(file_name)) {
			throw new Exception("ETL_Tool_DM_ParseFileName 建立檔名有誤!!\n");
		}
		

	}

	public String getCentral_no() {
		return central_no;
	}

	public void setCentral_no(String central_no) {
		this.central_no = central_no;
	}

	public String getFile_name() {
		return file_name;
	}

	public void setFile_name(String file_name) {
		this.file_name = file_name;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getBatch_no() {
		return batch_no;
	}

	public void setBatch_no(String batch_no) {
		this.batch_no = batch_no;
	}

	public Date getRecord_date() {
		return record_date;
	}

	public void setRecord_date(Date record_date) {
		this.record_date = record_date;
	}

	public String getRecord_date_str() {
		return record_date_str;
	}

	public void setRecord_date_str(String record_date_str) {
		this.record_date_str = record_date_str;
	}
	
//	public static  void main(String [] args) throws Exception {
//		ETL_Tool_DM_ParseFileName obj = new ETL_Tool_DM_ParseFileName("TR_600_BRANCHMAPPING_20180510.txt");
//		
//		System.out.println(obj.getFile_name());
//		System.out.println(obj.getFileName());
//		
//		obj = new ETL_Tool_DM_ParseFileName("TR_600_IDMAPPING_20180510.txt");
//		
//		System.out.println(obj.getFile_name());
//		System.out.println(obj.getFileName());
//		
//		obj = new ETL_Tool_DM_ParseFileName("TR_600_ACCTMAPPING_20180510.txt");
//		
//		System.out.println(obj.getFile_name());
//		System.out.println(obj.getFileName());
//	
//	}
}
