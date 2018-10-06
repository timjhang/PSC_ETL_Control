package Control;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.ibm.db2.jcc.DB2Types;

import Bean.ETL_Bean_LogData;
import Bean.ETL_Bean_VerificationCentral;
import DB.ConnectionHelper;
import Profile.ETL_Profile;
import Verification.ETL_V_Verification;

public class ETL_C_Verification {

	public static void execute() {
		System.out.println("#### ETL_C_Verification - Start  " +  new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		
		try {
		
			Date record_date = ETL_C_Master.getBeforeRecordDate(new Date());
			
			List<ETL_Bean_VerificationCentral> centralList = getLastExecuteCentral(record_date);
			
			for (int i = 0; i < centralList.size(); i++) {
				ETL_Bean_VerificationCentral aData = centralList.get(i);
				
				ETL_Bean_LogData logData = new ETL_Bean_LogData();
				logData.setRECORD_DATE(record_date);
				logData.setBATCH_NO(aData.getBatch_no());
				logData.setCENTRAL_NO(aData.getCentral_no());
				logData.setUPLOAD_NO(aData.getUpload_no());
				ETL_V_Verification.callVerificationFunctionByL(logData);
			}
		
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		System.out.println("#### ETL_C_Verification - End  " +  new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
	}
	
	// 取得最新執行中心資料
	private static List<ETL_Bean_VerificationCentral> getLastExecuteCentral(Date record_date) {
		List<ETL_Bean_VerificationCentral> resultList = new ArrayList<ETL_Bean_VerificationCentral>();
		
		Connection con = null;
		CallableStatement cstmt = null;
		
		try {
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Verification.getLastExecuteCentralData(?,?,?,?)}";
			
			con = ConnectionHelper.getDB2Connection();
			cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setDate(2, new java.sql.Date(record_date.getTime()));
			cstmt.registerOutParameter(3, DB2Types.CURSOR);
			cstmt.registerOutParameter(4, Types.VARCHAR);

			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(4);
				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
				
				return resultList;
			}
			
			java.sql.ResultSet rs = (java.sql.ResultSet) cstmt.getObject(3);
			while (rs.next()) {
				String batch_no = rs.getString(1);
				String central_no = rs.getString(2);
				String upload_no = rs.getString(3);
				
				ETL_Bean_VerificationCentral aData = new ETL_Bean_VerificationCentral();
				aData.setBatch_no(batch_no);
				if (central_no != null) {
					aData.setCentral_no(central_no.trim());
				}
				aData.setUpload_no(upload_no);
				
				resultList.add(aData);
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				// 資源後開,先關
				if (cstmt != null) {
					cstmt.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		return resultList;
	}
	
}
