package DB;

import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import Tool.ETL_Tool_CastObjUtil;

import com.ibm.db2.jcc.am.SqlDataException;

/**
 * 
 * @author Kevin 將資料寫入db2
 */
public class ETL_P_Data_Writer {

	// 用Array Type將資料寫入DB
	public static <T> boolean insertByDefineArrayListObject(List<T> javaBeans, InsertAdapter insertAdapter) {

		boolean isSucess = true;
		int typeArrayLength = insertAdapter.getTypeArrayLength();
		final String sql = insertAdapter.getSql();
		Connection con = null;
		CallableStatement cstmt = null;
		try {
			// 取得連線
			con = ConnectionHelper.getDB2Connection();

			// 關閉自動提交
			con.setAutoCommit(false);

			// 選擇要呼叫的SQL
			cstmt = con.prepareCall(sql);

			// 用來存放要轉換成Struct的List 每typeArrayLength筆當成一個List
			List<Object[]> insertArrs = new ArrayList<Object[]>();

			// 用來存放要轉換成Struct的List 因為會超過ArrayType的轉換長度,所以要另外創個insertArrs做成動態的
			List<Object[]> allObjectArr = ETL_Tool_CastObjUtil.castObjectArr(javaBeans);

			// 走訪全部
			for (Object[] objArr : allObjectArr) {
				// 塞入insertArrs
				insertArrs.add(objArr);
				try {

					// 當走訪的長度大於type長度時做轉換成代表type的Array 並insert
					if (insertArrs.size() >= typeArrayLength) {
						Struct[] structArr = new Struct[typeArrayLength];
						
						//ObjectArrs轉換成structArrs
						for (int i = 0; i < structArr.length; i++) {
							//將Object[]的轉換成Struct		 ps:一個Object[] =代表一個 row
							structArr[i] = con.createStruct(insertAdapter.getCreateStructTypeName(), insertArrs.get(i));
						}

						//structArrs轉換成Array
						Array array = con.createArrayOf(insertAdapter.getCreateStructTypeName(), structArr);

						cstmt.setArray(1, array);
						
						cstmt.execute();
						
						con.commit();
						
						insertArrs.clear();
					}
				} catch (SqlDataException e) {
					e.printStackTrace();
					isSucess = false;
					con.rollback();
					insertArrs.clear();
				}

			}

			try {

				// 將剩餘的Object轉換成代表type的Array 並insert
				if (insertArrs.size() > 0) {
					Struct[] structArr = new Struct[insertArrs.size()];
					for (int i = 0; i < structArr.length; i++) {
						structArr[i] = con.createStruct(insertAdapter.getCreateStructTypeName(), insertArrs.get(i));
					}

					Array array = con.createArrayOf(insertAdapter.getCreateStructTypeName(), structArr);

					cstmt.setArray(1, array);
					cstmt.execute();
					con.commit();
					insertArrs.clear();
				}

			} catch (SqlDataException e) {
				e.printStackTrace();
				isSucess = false;
				con.rollback();
				insertArrs.clear();
			}
		} catch (Exception e) {
			e.printStackTrace();
			isSucess = false;
		} finally {
			try {
				if (con != null) {
					con.close();
				}
				if (cstmt != null) {
					cstmt.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
				System.out.print("Error:" + e.getMessage());
			}
		}

		return isSucess;
	}
	
	// 用Array Type將資料寫入DB, 回傳寫入失敗筆數資料(值寫入InsertAdapter內)
	public static <T> boolean insertByDefineArrayListObject2(List<T> javaBeans, InsertAdapter insertAdapter) {

		boolean isSucess = true;
		int typeArrayLength = insertAdapter.getTypeArrayLength();
		final String sql = insertAdapter.getSql();
		Connection con = null;
		CallableStatement cstmt = null;
		try {
			// 取得連線
			con = ConnectionHelper.getDB2Connection();

			// 關閉自動提交
			con.setAutoCommit(false);

			// 選擇要呼叫的SQL
			cstmt = con.prepareCall(sql);

			// 用來存放要轉換成Struct的List 每typeArrayLength筆當成一個List
			List<Object[]> insertArrs = new ArrayList<Object[]>();

			// 用來存放要轉換成Struct的List 因為會超過ArrayType的轉換長度,所以要另外創個insertArrs做成動態的
			List<Object[]> allObjectArr = ETL_Tool_CastObjUtil.castObjectArr(javaBeans);

			// 走訪全部
			for (Object[] objArr : allObjectArr) {
				// 塞入insertArrs
				insertArrs.add(objArr);
				try {

					// 當走訪的長度大於type長度時做轉換成代表type的Array 並insert
					if (insertArrs.size() >= typeArrayLength) {
						Struct[] structArr = new Struct[typeArrayLength];
						
						//ObjectArrs轉換成structArrs
						for (int i = 0; i < structArr.length; i++) {
							//將Object[]的轉換成Struct		 ps:一個Object[] =代表一個 row
							structArr[i] = con.createStruct(insertAdapter.getCreateStructTypeName(), insertArrs.get(i));
						}

						//structArrs轉換成Array
						Array array = con.createArrayOf(insertAdapter.getCreateArrayTypesName(), structArr);

						cstmt.setArray(1, array);
						cstmt.registerOutParameter(2, Types.INTEGER); // 取得回傳寫入失敗筆數
						
						cstmt.execute();
						con.commit();
						
						// 取得寫入失敗筆數
						int errorCount = cstmt.getInt(2);
						System.out.println("errorCount = " + errorCount); // test  temp
						// 存入錯誤筆數
						insertAdapter.setErrorCount(errorCount);
						
						insertArrs.clear();
					}
				} catch (SqlDataException e) {
					e.printStackTrace();
					isSucess = false;
					con.rollback();
					insertArrs.clear();
				}

			}

			try {

				// 將剩餘的Object轉換成代表type的Array 並insert
				if (insertArrs.size() > 0) {
					Struct[] structArr = new Struct[insertArrs.size()];
					for (int i = 0; i < structArr.length; i++) {
						structArr[i] = con.createStruct(insertAdapter.getCreateStructTypeName(), insertArrs.get(i));
					}

					Array array = con.createArrayOf(insertAdapter.getCreateStructTypeName(), structArr);

					cstmt.setArray(1, array);
					cstmt.registerOutParameter(2, Types.INTEGER); // 取得回傳寫入失敗筆數
					
					cstmt.execute();
					con.commit();
					
					// 取得寫入失敗筆數
					int errorCount = cstmt.getInt(2);
					System.out.println("2 errorCount = " + errorCount); // test  temp
					// 存入錯誤筆數
					insertAdapter.setErrorCount(errorCount);
					
					insertArrs.clear();
				}

			} catch (SqlDataException e) {
				e.printStackTrace();
				isSucess = false;
				con.rollback();
				insertArrs.clear();
			}
		} catch (Exception e) {
			e.printStackTrace();
			isSucess = false;
		} finally {
			try {
				if (con != null) {
					con.close();
				}
				if (cstmt != null) {
					cstmt.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
				System.out.print("Error:" + e.getMessage());
			}
		}

		return isSucess;
	}

}
