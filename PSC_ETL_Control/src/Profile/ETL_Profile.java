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

	private final static String db2SPSchema = ETL_Tool_DES.decrypt("29ee04b861a87da1");
	public final static String db2TableSchema = ETL_Tool_DES.decrypt("29ee04b861a87da1");
	public final static String db2Ip = ETL_Tool_DES.decrypt("ad39f2476756e4bfbe5b2f87aa306a31");
	public final static String db2port = ETL_Tool_DES.decrypt("53000740a6adaa0d");

	public final static String db2Url = 
			ETL_Tool_DES.decrypt("58fcd8a8d6fa0357b43596ad4e09a6c4493ff949a"
					+ "610bfc777432c83a5696d7645fb4928d65fcffb14ecc533031ffd"
					+ "1ea810009434c9f055571abf832b087a8c949513758b0ed27dd81b3b4aede62aba");

	public final static String db2User = ETL_Tool_DES.decrypt("0f62db871d1d0101");
	public final static String db2Password = ETL_Tool_DES.decrypt("44ee592ddae3f0d2c53af1a4a004b34f");
	
	
	public final static String GAML_db2User = ETL_Tool_DES.decrypt("0f62db871d1d0101");
	public final static String GAML_db2Password = ETL_Tool_DES.decrypt("44ee592ddae3f0d2c53af1a4a004b34f");
	public final static String GAML_db2TableSchema = ETL_Tool_DES.decrypt("29ee04b861a87da1");
	private final static String GAML_db2SPSchema = ETL_Tool_DES.decrypt("29ee04b861a87da1");
	public final static String db2UrlGAMLpre = 
			ETL_Tool_DES.decrypt("58fcd8a8d6fa0357b43596ad4e09a6c4493f"
					+ "f949a610bfc777432c83a5696d76a4a51f2d50ba1358");
	public final static String db2UrlGAMLafter = 
			ETL_Tool_DES.decrypt("210854254ff94eec32f326325a431d9a248f310"
					+ "40be3a7193b4f37cefd0fe16ddafed100689a1ac85961605e87949653");
	
	
	// Error Log寫入域值
	public final static int ErrorLog_Stage = 10000;
	
	// Data 寫入域值
	public final static int Data_Stage = 10000;
	
	// 業務別
	public final static String Foreign_Currency = "FR"; // 外幣
	
	// 難字表excel檔存放路徑
	public final static String DifficultWords_Lists_Path = 
			ETL_Tool_DES.decrypt("8ba6ba4bf4a45186b96fcc12bd0144"
					+ "5545021e9d1fe4431eae94124ee542db4e160dd19b9ddd3c9a");

	// 特殊符號及罕見字表excel檔存放路徑
	public final static String SpecialWords_Lists_Path = 
			ETL_Tool_DES.decrypt("8ba6ba4bf4a45186a3850695ef0ab3b2c"
					+ "085821dab27407e0ff582606aa619f62581dc1e31994710893522724e52e2a9160dd19b9ddd3c9a");
	
	// 新北市農會附設北區農會電腦共用中心  951  相關參數
	
	// 財團法人農漁會南區資訊中心  952  相關參數
	
	// 板橋區農會電腦共用中心  928  相關參數
	
	// 財團法人農漁會聯合資訊中心  910  相關參數
	
	// 高雄市農會  605  相關參數
	
	// 農漁會資訊共用系統  600  相關參數
	
	// 018金庫  018  相關參數
	
	
	public static String getDB2Url(String v_CENTRAL_NO) {
		String db2url = 
				ETL_Tool_DES.decrypt("58fcd8a8d6fa0357b43596"
						+ "ad4e09a6c4493ff949a610bfc777432c83a5696d76a4a51f2d50ba1358")
				+ v_CENTRAL_NO 
				+ ETL_Tool_DES.decrypt("210854254ff94eec32f32"
						+ "6325a431d9a248f31040be3a7193b4f37ce"
						+ "fd0fe16ddafed100689a1ac85961605e87949653");
		
		return db2url;
	}

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
	public final static String ETL_E_CLP_MODEL_SCRIPT_FILE_PATH = 
			ETL_Tool_DES.decrypt("021d9d8f41af834dbbd0cc67793869640ea2113bfe78e14e");
	public final static String ETL_E_CLP_RUN_SCRIPT_FILE_PATH = 
			ETL_Tool_DES.decrypt("021d9d8f41af834dbbd0cc67793869649ee8d199407624c0");
	
	// for test
	public final static String Before_Record_Date_Str = "20180521";
	public final static String Record_Date_Str = "20180522";
	
}
