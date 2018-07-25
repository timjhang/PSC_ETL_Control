package Migration;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Types;
import Bean.ETL_Bean_DM_LogData;
import DB.ConnectionHelper;
import Profile.ETL_Profile;
import Tool.ETL_Tool_CastObjUtil;

public class ETL_DM_ALERT_PARTY_EMAIL extends Migration {
	public ETL_DM_ALERT_PARTY_EMAIL() {
	}

	public ETL_DM_ALERT_PARTY_EMAIL(ETL_Bean_DM_LogData logData) {
		super(logData);
	}

	@Override
	public void migration_File() {
		System.out.println("#######Migration - ETL_DM_ALERT_PARTY_EMAIL - Start");
		Connection con = null;
		CallableStatement cstmt = null;
		try {
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Migration.ETL_DM_ALERT_PARTY_EMAIL(?,?,?)}";
			con = ConnectionHelper.getDB2Connection(logData.getCentral_no());
			cstmt = con.prepareCall(sql);
			Struct dataStruct = con.createStruct("T_ETL_FILE_LOG", ETL_Tool_CastObjUtil.castObjectArr(logData));
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.registerOutParameter(2, Types.VARCHAR);
			cstmt.setObject(3, dataStruct);
			cstmt.execute();
			int returnCode = cstmt.getInt(1);
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(2);
				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}finally {
			try {
				// 資源後開,先關
				if (cstmt != null) {
					cstmt.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} 
		System.out.println("#######Migration - ETL_DM_ALERT_PARTY_EMAIL - End");
	}
}