package Load;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Struct;
import java.sql.Types;

import Bean.ETL_Bean_LogData;
import DB.ConnectionHelper;
import Profile.ETL_Profile;
import Tool.ETL_Tool_CastObjUtil;

public class ETL_L_ERROR_LOG extends Load {

	public ETL_L_ERROR_LOG() {
		
	}

	public ETL_L_ERROR_LOG(ETL_Bean_LogData logData, String fedServer, String runTable) {
		super(logData, fedServer, runTable);
	}

	@Override
	public void load_File() {
		try {
			trans_to_Error_Log(this.logData, this.fedServer, this.runTable);
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
	// 觸發DB2載入Procedure, 資料載入ERROR_LOG
	public void trans_to_Error_Log(ETL_Bean_LogData logData, String fedServer, String runTable) {
		
		System.out.println("#######Load - ETL_L_ERROR_LOG - Start");
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Load.loadETL_Error_Log(?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection(logData.getCENTRAL_NO().trim());
			CallableStatement cstmt = con.prepareCall(sql);
			
			Struct dataStruct = con.createStruct("T_LOGDATA", ETL_Tool_CastObjUtil.castObjectArr(logData));
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setObject(2, dataStruct);
			cstmt.setString(3, fedServer);			
			cstmt.registerOutParameter(4, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(4);
	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		System.out.println("#######Load - ETL_L_ERROR_LOG - End");
		
	}
	
}
