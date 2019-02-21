package Profile;

import Tool.ETL_Tool_DES;

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
	public final static String db2Ip = "172.18.21.207";
	public final static String db2port = "50000";

	public final static String db2Url = 
			"jdbc:db2://" + db2Ip + ":" + db2port + "/GAMLDB:" + "currentschema="
			+ db2SPSchema + ";" + "currentFunctionPath=" + db2SPSchema + ";";

	public final static String db2User = "GAMLETL";
	public final static String db2Password = "1qaz@WSX";
	
//	private final static String db2SPSchema = ETL_Tool_DES.decrypt("29ee04b861a87da1");
//	public final static String db2TableSchema = ETL_Tool_DES.decrypt("29ee04b861a87da1");
//	public final static String db2Ip = ETL_Tool_DES.decrypt("ad39f2476756e4bfbe5b2f87aa306a31");
//	public final static String db2port = ETL_Tool_DES.decrypt("53000740a6adaa0d");

//	public final static String db2Url = 
//			ETL_Tool_DES.decrypt("58fcd8a8d6fa0357b43596ad4e09a6c4493ff949a"
//					+ "610bfc777432c83a5696d7645fb4928d65fcffb14ecc533031ffd"
//					+ "1ea810009434c9f055571abf832b087a8c949513758b0ed27dd81b3b4aede62aba");
//
//	public final static String db2User = ETL_Tool_DES.decrypt("0f62db871d1d0101");
//	public final static String db2Password = ETL_Tool_DES.decrypt("44ee592ddae3f0d2c53af1a4a004b34f");
	
	
	// 連線GAML用URL string(Web Service系列程式, 連線用版本)
	public final static String GAML_db2User = "GAMLETL";
	public final static String GAML_db2Password = "1qaz@WSX";
	public final static String GAML_db2TableSchema = "SRC";
	private final static String GAML_db2SPSchema = "SRC";
	public final static String db2UrlGAMLpre = "jdbc:db2://172.18.21.207:50000/GAML";
	public final static String db2UrlGAMLafter = 
			":currentschema=" + GAML_db2SPSchema + ";" +
			"currentFunctionPath=" + GAML_db2SPSchema + ";";
	
//	public final static String GAML_db2User = ETL_Tool_DES.decrypt("0f62db871d1d0101");
//	public final static String GAML_db2Password = ETL_Tool_DES.decrypt("44ee592ddae3f0d2c53af1a4a004b34f");
//	public final static String GAML_db2TableSchema = ETL_Tool_DES.decrypt("29ee04b861a87da1");
//	private final static String GAML_db2SPSchema = ETL_Tool_DES.decrypt("29ee04b861a87da1");
//	public final static String db2UrlGAMLpre = 
//			ETL_Tool_DES.decrypt("58fcd8a8d6fa0357b43596ad4e09a6c4493f"
//					+ "f949a610bfc777432c83a5696d76a4a51f2d50ba1358");
//	public final static String db2UrlGAMLafter = 
//			ETL_Tool_DES.decrypt("210854254ff94eec32f326325a431d9a248f310"
//					+ "40be3a7193b4f37cefd0fe16ddafed100689a1ac85961605e87949653");
	
	
	// Error Log寫入域值
	public final static int ErrorLog_Stage = 10000;
	
	// Data 寫入域值
	public final static int Data_Stage = 10000;
	
	// 業務別
	public final static String Foreign_Currency = "FR"; // 外幣
	
	// 難字表excel檔存放路徑
	public final static String DifficultWords_Lists_Path = "C:/AML_TOOL/DifficultWords/%s.xlsx";
	
//	public final static String DifficultWords_Lists_Path = 
//			ETL_Tool_DES.decrypt("8ba6ba4bf4a45186b96fcc12bd0144"
//					+ "5545021e9d1fe4431eae94124ee542db4e160dd19b9ddd3c9a");

	// 特殊符號及罕見字表excel檔存放路徑
	public final static String SpecialWords_Lists_Path = "C:/AML_TOOL/SpecialBig5Words/SpecialBig5Words.xlsx";
	
//	public final static String SpecialWords_Lists_Path = 
//			ETL_Tool_DES.decrypt("8ba6ba4bf4a45186a3850695ef0ab3b2c"
//					+ "085821dab27407e0ff582606aa619f62581dc1e31994710893522724e52e2a9160dd19b9ddd3c9a");
	
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
	
//	public static String getDB2Url(String v_CENTRAL_NO) {
//		String db2url = 
//				ETL_Tool_DES.decrypt("58fcd8a8d6fa0357b43596"
//						+ "ad4e09a6c4493ff949a610bfc777432c83a5696d76a4a51f2d50ba1358")
//				+ v_CENTRAL_NO 
//				+ ETL_Tool_DES.decrypt("210854254ff94eec32f32"
//						+ "6325a431d9a248f31040be3a7193b4f37ce"
//						+ "fd0fe16ddafed100689a1ac85961605e87949653");
//		
//		return db2url;
//	}

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
	public final static int ETL_E_AGENT = 85;
	public final static int ETL_E_SCUSTBOX = 30;
	public final static int ETL_E_SCUSTBOXOPEN =379;
	public final static int ETL_E_SPARTY = 180;

	// 讀檔筆數域值
	public final static int ETL_E_Stage = 10000;
	
	// 五代用Script檔讀取位置
	public final static String ETL_E_CLP_MODEL_SCRIPT_FILE_PATH = "C:/ETL/L_Script/model/";
	public final static String ETL_E_CLP_RUN_SCRIPT_FILE_PATH = "C:/ETL/L_Script/run/";
	
//	public final static String ETL_E_CLP_MODEL_SCRIPT_FILE_PATH = 
//			ETL_Tool_DES.decrypt("021d9d8f41af834dbbd0cc67793869640ea2113bfe78e14e");
//	public final static String ETL_E_CLP_RUN_SCRIPT_FILE_PATH = 
//			ETL_Tool_DES.decrypt("021d9d8f41af834dbbd0cc67793869649ee8d199407624c0");
	
	// EMail 設定參數
	public final static String EMAIL_SERVER_HOST = "172.17.16.140";
	public final static int EMAIL_SERVER_PORT = 25;
	public final static String EMAIL_SERVER_ADDRESS ="AML_ETL@agribank.com";
	
	// ETL Server設定參數
	public final static String ETL_db2User = "ETLUSR";
	public final static String ETL_db2Password = "1qazXSW@";
	public final static String db2ETLTableSchema = "ETLUSR";
//	public final static String db2UrlETLpre1 = "jdbc:db2://172.18.6.151:50000/ETLDB";
	public final static String db2UrlETLpre1 = "jdbc:db2://172.18.21.206:50000/ETLDB";
	public final static String db2UrlETLpre2 = "jdbc:db2://172.18.6.152:50000/ETLDB";
	public final static String db2UrlETLafter = ":currentschema=" + db2ETLTableSchema + ";currentFunctionPath=" + db2ETLTableSchema + ";";
	
//	public final static String ETL_db2User = ETL_Tool_DES.decrypt("2772602b4cd74bec30a2869c8e5426a6");
//	public final static String ETL_db2Password = ETL_Tool_DES.decrypt("2d8f563ca3253c20c53af1a4a004b34f");
//	public final static String db2ETLTableSchema = ETL_Tool_DES.decrypt("2772602b4cd74bec30a2869c8e5426a6");
//	public final static String db2UrlETLpre1 = ETL_Tool_DES.decrypt("58fcd8a8d6fa0357b43596ad4e09a6c44ab4f8a6772d57e1925e5a9ab1fb66c8e1bc3ec9dd2b26a3");
//	public final static String db2UrlETLpre2 = ETL_Tool_DES.decrypt("58fcd8a8d6fa0357b43596ad4e09a6c46d3311bad199a52e925e5a9ab1fb66c8e1bc3ec9dd2b26a3");
//	public final static String db2UrlETLafter = 
//			ETL_Tool_DES.decrypt("210854254ff94eec0071e43998c89a28") + db2ETLTableSchema +
//			ETL_Tool_DES.decrypt("99997715abeec0bcc2d843c1f1791567e444209833d7b9a6") + db2ETLTableSchema + ETL_Tool_DES.decrypt("1cf4f9def8d8492f");
	
	// for test
	public final static String Before_Record_Date_Str = "20180802";
	public final static String Record_Date_Str = "20180803";
	
}
