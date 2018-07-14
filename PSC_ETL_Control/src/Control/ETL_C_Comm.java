package Control;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Struct;
import java.sql.Types;

import Bean.ETL_Bean_LogData;
import DB.ConnectionHelper;
import Profile.ETL_Profile;
import Tool.ETL_Tool_CastObjUtil;

public class ETL_C_Comm {
	
	// 擔保品, 保證人檔對party進行補漏作業
	public static void SupplementParty (ETL_Bean_LogData logData, String fedServer, String runTable) {
		
		System.out.println("#######ETL_C_Comm - SupplementParty - Start");
		
		try {
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Supplement.supplementParty(?,?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection(logData.getCENTRAL_NO().trim());
			CallableStatement cstmt = con.prepareCall(sql);
			
			Struct dataStruct = con.createStruct("T_LOGDATA", ETL_Tool_CastObjUtil.castObjectArr(logData));
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setObject(2, dataStruct);
			cstmt.setObject(3, fedServer);
			cstmt.setObject(4, runTable);
			cstmt.registerOutParameter(5, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(5);
	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		System.out.println("#######ETL_C_Comm - SupplementParty - End");
		
	}

	public static void main(String[] args) {
		
		System.out.println("測試開始  Start");
		
		ETL_Bean_LogData logData = new ETL_Bean_LogData();
		logData.setCENTRAL_NO("018");
		
		SupplementParty(logData, null, "TEMP");
		
		System.out.println("測試結束  End");

	}

}
