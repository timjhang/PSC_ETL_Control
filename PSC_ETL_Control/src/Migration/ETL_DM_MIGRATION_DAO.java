package Migration;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Types;
import java.util.Date;

import Bean.ETL_Bean_LogData;
import Bean.ETL_Bean_Response;
import DB.ConnectionHelper;
import DB.ETL_P_Log;
import Profile.ETL_Profile;
import Tool.ETL_Tool_CastObjUtil;
import Tool.ETL_Tool_StringX;

public class ETL_DM_MIGRATION_DAO {

	// 取得Migration_Status數量
	public static int get_Migration_Status_Count(Date record_date, String central_no, String Status) {
		Connection con = null;
		CallableStatement cstmt = null;

		try {
			String sql = "{call " + ETL_Profile.db2TableSchema + ".DM.get_Migration_Status_Count(?,?,?,?,?,?)}";

			con = ConnectionHelper.getDB2Connection();
			cstmt = con.prepareCall(sql);

			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setDate(2, new java.sql.Date(record_date.getTime()));
			cstmt.setString(3, central_no);
			cstmt.setString(4, Status);
			cstmt.registerOutParameter(5, Types.INTEGER);
			cstmt.registerOutParameter(6, Types.VARCHAR);

			cstmt.execute();

			int returnCode = cstmt.getInt(1);

			// 有錯誤釋出錯誤訊息 回傳0
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(6);
				System.out.println("####DM - Error Code = " + returnCode + ", Error Message : " + errorMessage);
				ETL_P_Log.write_Runtime_Log("DM", "####get_Migration_Status_Count: - Error Code = " + returnCode + ", Error Message : " + errorMessage);
				return 0;
			}

			int StatusCount = cstmt.getInt(5);

			return StatusCount;
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("DM", ex.getMessage());
			return 0;
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}
	
	// 取得Migration_Status
	public static ETL_Bean_Response get_Migration_Status(Date record_date, String central_no) {
		Connection con = null;
		CallableStatement cstmt = null;
		String status = null;
		ETL_Bean_Response res = new ETL_Bean_Response();

		try {
			String sql = "{call " + ETL_Profile.db2TableSchema + ".DM.get_Migration_Status(?,?,?,?,?)}";

			con = ConnectionHelper.getDB2Connection();
			cstmt = con.prepareCall(sql);

			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setDate(2, new java.sql.Date(record_date.getTime()));
			cstmt.setString(3, central_no);
			cstmt.registerOutParameter(4, Types.VARCHAR);
			cstmt.registerOutParameter(5, Types.VARCHAR);

			cstmt.execute();

			int returnCode = cstmt.getInt(1);

			// 有錯誤釋出錯誤訊息 回傳0
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(5);
				System.out.println("####DM - Error Code = " + returnCode + ", Error Message : " + errorMessage);
				ETL_P_Log.write_Runtime_Log("DM", "####get_Migration_Status_Count: - Error Code = " + returnCode + ", Error Message : " + errorMessage);
				
				res.setError("####get_Migration_Status - Error Code = " + returnCode + ", Error Message : " + errorMessage);
				
				return res;
			}

			status = cstmt.getString(4);
			res.setSuccessObj(status);

			return res;
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("DM", ex.getMessage());
			res.setError(ex.getMessage());
			return res;
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	// 觸發DB2載入Procedure, 資料載入BRANCHMAPPING_LOAD
	public static void trans_to_BRANCHMAPPING_LOAD(ETL_Bean_LogData logData, String fedServer, String runTable) {
		Connection con = null;
		CallableStatement cstmt = null;

		try {

			String sql = "{call " + ETL_Profile.db2TableSchema + ".DM.loadETL_BRANCHMAPPING(?,?,?,?,?)}";

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
				ETL_P_Log.write_Runtime_Log("DM", " trans_to_BRANCHMAPPING_LOAD : Error Code = " + returnCode + ", Error Message : " + errorMessage);
			}

		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("DM", ex.getMessage());
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	// 觸發DB2載入Procedure, 資料載入ACCTMAPPING_LOAD
	public static void trans_to_ACCTMAPPING_LOAD(ETL_Bean_LogData logData, String fedServer, String runTable) {
		Connection con = null;
		CallableStatement cstmt = null;
		try {

			String sql = "{call " + ETL_Profile.db2TableSchema + ".DM.loadETL_ACCTMAPPING(?,?,?,?,?)}";

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
				ETL_P_Log.write_Runtime_Log("DM", " trans_to_ACCTMAPPING_LOAD : Error Code = " + returnCode + ", Error Message : " + errorMessage);
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}
	
	// 觸發DB2載入Procedure, 資料載入IDMAPPING_LOAD
	public static void trans_to_IDMAPPING_LOAD(ETL_Bean_LogData logData, String fedServer, String runTable) {
		Connection con = null;
		CallableStatement cstmt = null;
		try {

			String sql = "{call " + ETL_Profile.db2TableSchema + ".DM.loadETL_IDMAPPING(?,?,?,?,?)}";

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
				ETL_P_Log.write_Runtime_Log("DM", " trans_to_IDMAPPING_LOAD : Error Code = " + returnCode + ", Error Message : " + errorMessage);
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	// 檢查checkMappingData
	public static boolean checkMappingData(String central_no, String batch_No) {
		boolean isSuccess = false;
		Connection con = null;
		CallableStatement cstmt = null;
		// error
		// etl_file_log
		String sql = "{call " + ETL_Profile.db2TableSchema + ".DM.checkMappingData(?,?,?,?)}";

		try {
			con = ConnectionHelper.getDB2Connection(central_no);

			cstmt = con.prepareCall(sql);

			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setString(2, central_no);
			cstmt.setString(3, batch_No);
			cstmt.registerOutParameter(4, Types.VARCHAR);

			cstmt.execute();

			int returnCode = cstmt.getInt(1);

			// 有錯誤釋出錯誤訊息 不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(4);
				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
				ETL_P_Log.write_Runtime_Log("DM", " checkMappingData : Error Code = " + returnCode + ", Error Message : " + errorMessage);
				isSuccess = false;
			} else {
				isSuccess = true;
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			isSuccess = false;
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		return isSuccess;

	}

	// 更新5代Table 記錄檔 MigrationStatus
	public static boolean updateNewGenerationMigrationStatus(Date record_date, String central_no, String migration_status, String batch_No) {
		boolean isSuccess = false;
		Connection con = null;
		CallableStatement cstmt = null;

		try {
			String sql = "{call " + ETL_Profile.db2TableSchema + ".DM.update_New_Generation_ETL_MIGRATION_STATUS(?,?,?,?,?,?)}";

			con = ConnectionHelper.getDB2Connection();
			cstmt = con.prepareCall(sql);

			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setDate(2, new java.sql.Date(record_date.getTime()));
			cstmt.setString(3, central_no);
			cstmt.setString(4, migration_status);
			cstmt.setString(5, batch_No);
			cstmt.registerOutParameter(6, Types.VARCHAR);

			cstmt.execute();

			int returnCode = cstmt.getInt(1);

			// 有錯誤釋出錯誤訊息 不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(6);
				System.out.println("####updateNewGenerationMigrationStatus - Error Code = " + returnCode + ", Error Message : " + errorMessage);
				ETL_P_Log.write_Runtime_Log("updateNewGenerationMigrationStatus", "####updateNewGenerationMigrationStatus - Error Code = " + returnCode + ", Error Message : " + errorMessage);
				isSuccess = false;
			} else {
				isSuccess = true;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("updateNewGenerationMigrationStatus", ex.getMessage());
			isSuccess = false;
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		return isSuccess;
	}

	// 更新5代Table 記錄檔 isRerun
	public static void update_New_Generation_isRerun(Date record_date, String central_no, int isRerun) {
		Connection con = null;
		CallableStatement cstmt = null;
		try {
			String sql = "{call " + ETL_Profile.db2TableSchema + ".DM.update_New_Generation_isRerun(?,?,?,?,?)}";

			con = ConnectionHelper.getDB2Connection();
			cstmt = con.prepareCall(sql);

			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setDate(2, new java.sql.Date(record_date.getTime()));
			cstmt.setString(3, central_no);
			cstmt.setInt(4, isRerun);
			cstmt.registerOutParameter(5, Types.VARCHAR);

			cstmt.execute();

			int returnCode = cstmt.getInt(1);

			// 有錯誤釋出錯誤訊息 不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(5);
				System.out.println("####DM - Error Code = " + returnCode + ", Error Message : " + errorMessage);
				ETL_P_Log.write_Runtime_Log("DM", "####DM - Error Code = " + returnCode + ", Error Message : " + errorMessage);
			}

		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("DM", ex.getMessage());

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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	// 取得Migration批次編號(序號表ETL_PARAMETER_INFO)
	public static String getMigration_BatchNo() {
		String result = "";
		Connection con = null;
		CallableStatement cstmt = null;

		try {

			String sql = "{call " + ETL_Profile.db2TableSchema + ".Migration.getMigration_BatchNo(?,?,?)}";

			con = ConnectionHelper.getDB2Connection();
			cstmt = con.prepareCall(sql);

			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.registerOutParameter(2, Types.VARCHAR);
			cstmt.registerOutParameter(3, Types.VARCHAR);

			cstmt.execute();

			int returnCode = cstmt.getInt(1);

			// 有錯誤釋出錯誤訊息 不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(3);
				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
				ETL_P_Log.write_Runtime_Log("getMigration_BatchNo", "Error Code = " + returnCode + ", Error Message : " + errorMessage);

				return result;
			}

			result = cstmt.getString(2);

		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("getMigration_BatchNo", ex.getMessage());
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return result;
	}
	// 取得trans_CentralCodePool
	public static boolean trans_CentralCodePool(Date record_date, String fed,String central_no, String  batch_No) {
		boolean isSuccess = false;
		Connection con = null;
		CallableStatement cstmt = null;

		try {
			String sql = "{call " + ETL_Profile.db2TableSchema + ".DM.trans_CentralCodePool(?,?,?,?,?,?)}";

			con = ConnectionHelper.getDB2Connection();
			cstmt = con.prepareCall(sql);

			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setDate(2, new java.sql.Date(record_date.getTime()));
			cstmt.setString(3, fed);
			cstmt.setString(4, batch_No);
			cstmt.setString(5, central_no);
			cstmt.registerOutParameter(6, Types.VARCHAR);

			cstmt.execute();

			int returnCode = cstmt.getInt(1);

			// 有錯誤釋出錯誤訊息 不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(6);
				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
				ETL_P_Log.write_Runtime_Log("trans_CentralCodePool", "Error Code = " + returnCode + ", Error Message : " + errorMessage);
				isSuccess = false;
			} else {
				isSuccess = true;
			}

		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("trans_CentralCodePool", ex.getMessage());
			isSuccess = false;
		}

		return isSuccess;
	}

	// synchronize_CentralCodePool
	public static boolean synchronize_CentralCodePool(Date record_date, String fed, String etldbNo) {
		boolean isSuccess = false;
		Connection con = null;
		CallableStatement cstmt = null;

		try {
			String sql = "{call " + ETL_Profile.db2ETLTableSchema + ".DM.synchronize_CentralCodePool(?,?)}";

			con = ConnectionHelper.getETLDB2Connection(etldbNo);
			cstmt = con.prepareCall(sql);

			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.registerOutParameter(2, Types.VARCHAR);

			cstmt.execute();

			int returnCode = cstmt.getInt(1);

			// 有錯誤釋出錯誤訊息 不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(2);
				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
				ETL_P_Log.write_Runtime_Log("synchronize_CentralCodePool", "Error Code = " + returnCode + ", Error Message : " + errorMessage);
				isSuccess = false;
			} else {
				isSuccess = true;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("trans_CentralCodePool", ex.getMessage());
			isSuccess = false;
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return isSuccess;
	}

	public static void main(String[] args) throws Exception {
		
		System.out.println("start");
		ETL_Bean_Response res = ETL_DM_MIGRATION_DAO.get_Migration_Status(ETL_Tool_StringX.toUtilDate("20180817"), "600");

//		ETL_Bean_Response res = new ETL_Bean_Response();
//		res.setSuccessObj(null);
		
		System.out.println(res.getObj());
		
	}

}
