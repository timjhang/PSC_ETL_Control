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
//	private final static String db2SPSchema = "ADMINISTRATOR";
//	public final static String db2TableSchema = "ADMINISTRATOR";
//	private final static String db2SPSchema = "SRC";
//	public final static String db2TableSchema = "SRC";
//	public final static String db2Url = 
//			"jdbc:db2://172.18.21.206:50000/ETLDB600:" +
//			"currentschema=" + db2SPSchema + ";" +
//			"currentFunctionPath=" + db2SPSchema + ";";
//	public final static String db2User = "Administrator";
//	public final static String db2Password = "9ol.)P:?";
//	public final static String db2Url = 
//			"jdbc:db2://localhost:50000/sample:" +
//			"currentschema=" + db2SPSchema + ";" +
//			"currentFunctionPath=" + db2SPSchema + ";";
//	public final static String db2User = "administrator";
//	public final static String db2Password = "1qaz@WSX";
	
	private final static String db2SPSchema = "SRC";
	public final static String db2TableSchema = "SRC";
	public final static String db2Ip = "172.18.6.133";
	public final static String db2port = "50000";

	// GAMLDB
	public final static String db2Url = 
			"jdbc:db2://" + db2Ip + ":" + db2port + "/GAMLDB:" + "currentschema="
			+ db2SPSchema + ";" + "currentFunctionPath=" + db2SPSchema + ";";

	public final static String db2User = "GAMLETL";
	public final static String db2Password = "1qaz@WSX";
	
//	public final static String db2Url =
//			"jdbc:db2://localhost:50000/SAMPLE:"+
//			"currentschema=" + db2SPSchema + ";" +
//			"currentFunctionPath=" + db2SPSchema + ";";
//	
//	public final static String db2User = "tibyby";
//	public final static String db2Password = "Nn125303960";
	
	// Error Log寫入域值
	public final static int ErrorLog_Stage = 10000;
	
	// Data 寫入域值
	public final static int Data_Stage = 10000;
	
	// 業務別
	public final static String Foreign_Currency = "FR"; // 外幣
	
	// 難字表excel檔存放路徑
	public final static String DifficultWords_Lists_Path = "C:/DifficultWords/%s.xlsx";
	
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
	
}
