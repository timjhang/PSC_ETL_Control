package Profile;

public class ETL_Profile {
	// ETL系列程式   設定檔  2017.12.07 TimJhang
	
	// **E階段設定參數
	
	// 基本檢核參數(開/關)
	public final static boolean BasicCheck = true;
	// 比對檢核參數(開/關)
	public final static boolean AdvancedCheck = true;
	
	// **連線DB2 設定參數(預計有 7 + 1組)
	
	// Driver, Url, User, Password
	public final static String db2Driver = "com.ibm.db2.jcc.DB2Driver";

	// 控制程式 & L系列程式, 連線用版本
	private final static String db2SPSchema = "SRC";
	public final static String db2TableSchema = "SRC";
	public final static String db2Ip = "172.18.6.133";
	public final static String db2port = "50000";

	public final static String db2Url = 
			"jdbc:db2://" + db2Ip + ":" + db2port + "/GAMLDB:" + "currentschema="
			+ db2SPSchema + ";" + "currentFunctionPath=" + db2SPSchema + ";";

	public final static String db2User = "GAMLETL";
	public final static String db2Password = "1qaz@WSX";
	
	
	// 連線GAML用URL string(Web Service系列程式, 連線用版本)
	public final static String GAML_db2User = "GAMLETL";
	public final static String GAML_db2Password = "1qaz@WSX";
	public final static String GAML_db2TableSchema = "SRC";
	private final static String GAML_db2SPSchema = "SRC";
	public final static String db2UrlGAMLpre = "jdbc:db2://172.18.6.133:50000/GAML";
	public final static String db2UrlGAMLafter = 
			":currentschema=" + GAML_db2SPSchema + ";" +
			"currentFunctionPath=" + GAML_db2SPSchema + ";";
	
	
	// Error Log寫入域值
	public final static int ErrorLog_Stage = 10000;
	
	// Data 寫入域值
	public final static int Data_Stage = 10000;
	
	// 業務別
	public final static String Foreign_Currency = "FR"; // 外幣
	
	// 難字表excel檔存放路徑
	public final static String DifficultWords_Lists_Path = "C:/AML_TOOL/DifficultWords/%s.xlsx";

	// 特殊符號及罕見字表excel檔存放路徑
	public final static String SpecialWords_Lists_Path = "C:/AML_TOOL/SpecialBig5Words/SpecialBig5Words.xlsx";
	
	// 新北市農會附設北區農會電腦共用中心  951  相關參數
	
	// 財團法人農漁會南區資訊中心  952  相關參數
	
	// 板橋區農會電腦共用中心  928  相關參數
	
	// 財團法人農漁會聯合資訊中心  910  相關參數
	
	// 高雄市農會  605  相關參數
	
	// 農漁會資訊共用系統  600  相關參數
	
	// 018金庫  018  相關參數
	
	// 其他
	public static String getDB2Url(String v_CENTRAL_NO) {
		String db2url = "jdbc:db2://" + db2Ip + ":" + db2port + "/GAML" + v_CENTRAL_NO + ":" + "currentschema="
				+ db2SPSchema + ";" + "currentFunctionPath=" + db2SPSchema + ";";
		return db2url;
	}

	// GAMLDB
//	public static String getDB2Url() {
//		String db2url = "jdbc:db2://" + db2Ip + ":" + db2port + "/GAMLDB:" + "currentschema=" + db2SPSchema + ";"
//				+ "currentFunctionPath=" + db2SPSchema + ";";
//		return db2url;
//	}
	
	// 各資料讀檔緩衝區大小
	public final static int ETL_E_PARTY = 310;
	public final static int ETL_E_PARTY_PARTY_REL = 101;
	public final static int ETL_E_PARTY_PHONE = 22;
	public final static int ETL_E_PARTY_ADDRESS = 63;
	public final static int ETL_E_ACCOUNT = 56;
	public final static int ETL_E_TRANSACTION = 242;
	public final static int ETL_E_TRANSACTION_OLD = 204;
	public final static int ETL_E_LOAN_DETAIL = 56;
	public final static int ETL_E_LOAN = 107;
	public final static int ETL_E_COLLATERAL = 101;
	public final static int ETL_E_GUARANTOR = 62;
	public final static int ETL_E_FX_RATE = 15;
	public final static int ETL_E_SERVICE = 717;
	public final static int ETL_E_TRANSFER = 361;
	public final static int ETL_E_FCX = 125;
	public final static int ETL_E_CALENDAR = 11;

	// 讀檔筆數域值
	public final static int ETL_E_Stage = 10000;
	
	// 五代用Script檔讀取位置
	public final static String ETL_E_CLP_MODEL_SCRIPT_FILE_PATH = "C:/ETL/L_Script/model/";
	public final static String ETL_E_CLP_RUN_SCRIPT_FILE_PATH = "C:/ETL/L_Script/run/";
	
}
