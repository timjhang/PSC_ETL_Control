package Load;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Struct;
import java.sql.Types;

import Bean.ETL_Bean_LogData;
import DB.ConnectionHelper;
import Profile.ETL_Profile;
import Tool.ETL_Tool_CastObjUtil;

public class ETL_L_BALANCE extends Load {
	
	public ETL_L_BALANCE() {
		
	}

	public ETL_L_BALANCE(ETL_Bean_LogData logData, String fedServer, String runTable) {
		super(logData, fedServer, runTable);
	}

	@Override
	public void load_File() {
		try {
			trans_to_BALANCE_LOAD(this.logData, this.fedServer, this.runTable);
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
	// 觸發DB2載入Procedure, 資料載入BALANCE_LOAD_TEMP  
	public void trans_to_BALANCE_LOAD(ETL_Bean_LogData logData, String fedServer, String runTable) {
		
		System.out.println("#######Load - ETL_L_BALANCE - Start");
		
		Connection con = null;
		CallableStatement cstmt = null;
		
		try {
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Load.loadETL_BALANCE_LOAD(?,?,?,?,?)}";
			
			con = ConnectionHelper.getDB2Connection(logData.getCENTRAL_NO().trim());
			cstmt = con.prepareCall(sql);
			
			Struct dataStruct = con.createStruct("T_LOGDATA", ETL_Tool_CastObjUtil.castObjectArr(logData));
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setObject(2, dataStruct);
			cstmt.setString(3, fedServer);
			cstmt.setString(4, runTable);
			cstmt.registerOutParameter(5, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(5);
	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (con != null) {
					con.close();
				}
				if (cstmt != null) {
					cstmt.close();
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		
		System.out.println("#######Load - ETL_L_BALANCE - End"); 
		
	}

}
