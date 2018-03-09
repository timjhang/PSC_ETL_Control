package Tool;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ETL_Tool_ParseFileName {
	// 拆解檔案名工具

	// 批次編號
	private String Batch_no;
	// 檔案名
	private String FileName;
	// 處理檔名
	private String File_Name;
	// 報送單位
	private String Central_No;
	// 業務名稱
	private String File_Type;
	// 檔案日期
	private Date Record_Date;
	// 檔案日期文字
	private String Record_Date_String;
	// 上傳批號(測試用)
	private String upload_no;
	
	// ETL_Tool_ParseFileName's Constructor
	public ETL_Tool_ParseFileName(String fileName) throws Exception {
		// 檔名格式  AAA_BB_(CCC)_yyyyMMdd.txt   test  temp (格式未定)
		// AAA:報送單位, BB:業務別, CCC:處理檔名
		
		// 檔名非空檢核
		if (fileName == null || "".equals(fileName)) {
			throw new Exception("ETL_Tool_ParseFileName 建立檔名有誤!!\n無檔名");
		}
		
		// 檔名"_"比較檢核
		if (fileName.split("\\_").length < 4) {
			throw new Exception("ETL_Tool_ParseFileName 建立檔名有誤!!\n檔名無\"_\"");
		}
		// 副檔名必要檢核  test temp (格式未定)
		if (fileName.split("\\.").length < 2) {
			throw new Exception("ETL_Tool_ParseFileName 建立檔名有誤!!\n檔名無副檔名");
		}
		
		this.FileName = fileName; // 輸入全檔名
		
		String[] ary = FileName.split("\\_");
		
		this.Central_No = ary[0] + "       ".substring(0, 7 - ary[0].length()); // 寫入報送單位
		this.File_Type = ary[1];  // 寫入業務別
		String mainName = fileName.split("\\.")[0];
		this.File_Name = mainName.substring(ary[0].length() + ary[1].length() + 2, mainName.length() - 9); // 寫入處裡檔名
		String source = mainName.substring(mainName.length() - 8, mainName.length());
		this.Record_Date_String = source; // 寫入檔案日期文字(yyyyMMdd)
		this.Record_Date = new SimpleDateFormat("yyyyMMdd").parse(source); // 寫入檔案日期
	}
	
	// ETL_Tool_ParseFileName's Constructor, 針對沒有業務別檔案使用(類似Constructor)
	public ETL_Tool_ParseFileName(String fileName, boolean noFile_Type) throws Exception {
		if (!noFile_Type) {
			throw new Exception("ETL_Tool_ParseFileName 確認為無業務別檔案使用!!");
		}
		
		// 檔名格式  AAA_(CCC)_yyyyMMdd.txt   test  temp (格式未定)
		// AAA:報送單位, CCC:處理檔名
		
		// 檔名非空檢核
		if (fileName == null || "".equals(fileName)) {
			throw new Exception("ETL_Tool_ParseFileName 建立檔名有誤!!\n無檔名");
		}
		
		// 檔名"_"比較檢核
		if (fileName.split("\\_").length < 3) {
			throw new Exception("ETL_Tool_ParseFileName 建立檔名有誤!!\n檔名無\"_\"");
		}
		// 副檔名必要檢核  test temp (格式未定)
		if (fileName.split("\\.").length < 2) {
			throw new Exception("ETL_Tool_ParseFileName 建立檔名有誤!!\n檔名無副檔名");
		}
		
		this.FileName = fileName; // 輸入全檔名
		
		String[] ary = FileName.split("\\_");
		
		this.Central_No = ary[0] + "       ".substring(0, 7 - ary[0].length()); // 寫入報送單位
		String mainName = fileName.split("\\.")[0];
		this.File_Name = mainName.substring(ary[0].length() + 1, mainName.length() - 9); // 寫入處裡檔名
		String source = mainName.substring(mainName.length() - 8, mainName.length());
		this.Record_Date_String = source; // 寫入檔案日期文字(yyyyMMdd)
		this.Record_Date = new SimpleDateFormat("yyyyMMdd").parse(source); // 寫入檔案日期
	}
	
	public String getFileName() {
		return FileName;
	}
	
	public String getCentral_No() {
		return Central_No;
	}
	
	public String getFile_Type() {
		return File_Type;
	}
	
	public Date getRecord_Date() {
		return Record_Date;
	}
	
	public String getFile_Name() {
		return File_Name;
	}

	public String getRecord_Date_String() {
		return Record_Date_String;
	}

	public String getBatch_no() {
		return Batch_no;
	}

	public void setBatch_no(String batch_no) {
		Batch_no = batch_no;
	}

	public String getUpload_no() {
		return upload_no;
	}

	public void setUpload_no(String upload_no) {
		this.upload_no = upload_no;
	}
	
}
