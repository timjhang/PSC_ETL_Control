import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import Bean.ETL_Bean_LogData;
import Control.ETL_C_PROCESS;
import Control.ETL_C_Profile;
import Load.ETL_L_CALENDAR;
import Load.ETL_L_PARTY_PHONE;

public class KevinTest {

	public static void main(String[] args) throws UnsupportedEncodingException, ParseException {
		L_TEST();
		
		
	}
	
	public static void test1() throws UnsupportedEncodingException {
		// TODO Auto-generated method stub
		  String encodedURL = URLEncoder.encode(ETL_C_Profile.ETL_Download_localPath+"/"+"123"+"/"+"456", "UTF-8");
		  
		  
		  
		encodedURL = URLDecoder.decode(encodedURL, "UTF-8").trim();
	
	System.out.println(encodedURL);
	}
	
	public static void L_TEST() throws ParseException {

		String fedServer = "";
		// 設定fedName

		fedServer = "ETLDB002";

		// 執行用Table (正常 rerun, 重跑rerun)
		String runTable = "temp";

		ETL_Bean_LogData logData = new ETL_Bean_LogData();
		logData.setBATCH_NO("001");
		logData.setCENTRAL_NO("600");
		logData.setFILE_TYPE("tw");
		logData.setPROGRAM_NO("001");

		logData.setRECORD_DATE(new SimpleDateFormat("yyyyMMdd").parse("20171206"));
		logData.setUPLOAD_NO("001");
		
//		ETL_C_PROCESS.exeLfunction("ETL_S2", "001", "600", "20171206", "999", "20180331");
		
	}

}
