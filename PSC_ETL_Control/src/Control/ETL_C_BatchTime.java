package Control;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Types;
//import java.util.Calendar;

import DB.ConnectionHelper;
import Profile.ETL_Profile;

public class ETL_C_BatchTime {

	public static boolean isExecute(String strTime, String batchTypeCode) {
		
		System.out.println("####### ETL_C_BatchTime - isExecute - Start");
		
		int need = 0;
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".BATCH_SERVICE.needExecute(?,?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setString(2, strTime);
			cstmt.setString(3, batchTypeCode);
			cstmt.registerOutParameter(4, Types.INTEGER);
			cstmt.registerOutParameter(5, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(5);
	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			}
			
			need = cstmt.getInt(4);
			
		} catch (Exception ex) {
			ex.printStackTrace();
//			throw new Exception(ex.getMessage());
		}
		
		System.out.println("####### ETL_C_BatchTime - isExecute - End");
		
		if (need == 0) {
  			return false;
  		} else {
  			return true;
  		}
		
	}
	
}
