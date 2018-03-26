package DB;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ibm.db2.jcc.DB2Types;

import Bean.ETL_Bean_CodeName_Data;
import Profile.ETL_Profile;

public class ETL_Q_ColumnCheckCodes {
	
	// 母確認Map(取得單一Extract程式下, 所有欄位檢核用Map)
	private Map<String, Map<String, String>> checkMaps = new HashMap<String, Map<String, String>>();
	
	// 取得檢核用代碼List
	public static List<ETL_Bean_CodeName_Data> getCheckList(String columnName) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		
		List<ETL_Bean_CodeName_Data> resultList = new ArrayList<ETL_Bean_CodeName_Data>();
		
		Connection con = ConnectionHelper.getDB2Connection();
		
		java.sql.Statement stmt = con.createStatement();
		String query = "SELECT CODE, NAME FROM " + ETL_Profile.db2TableSchema + ".CODEPOOL "
				+ "WHERE TABLESPACENAME = \'" + columnName + "\'";
        java.sql.ResultSet rs = stmt.executeQuery(query);
        
        while (rs.next()) {
        	ETL_Bean_CodeName_Data data = new ETL_Bean_CodeName_Data();
        	data.setCode(rs.getString(1));
        	data.setName(rs.getString(2));
        	
        	System.out.println(data.getCode() + " " + data.getName()); // test
        	resultList.add(data);
        }
        
        System.out.println("List Size = " + resultList.size()); // test
		
		return resultList;
	}
	
	// 取得檢核用代碼Map
	public static Map<String, String> getCheckMap(Date record_date, String central_No, String columnName) throws Exception {
		
		// Code & Name
		Map<String, String> resultMap = new HashMap<String, String>();
		
        String sql = "{call " + ETL_Profile.db2TableSchema + ".Tool.getCodePairs(?,?,?,?,?,?)}";
		
		Connection con = ConnectionHelper.getDB2Connection();
		CallableStatement cstmt = con.prepareCall(sql);
		
		cstmt.registerOutParameter(1, Types.INTEGER);
		cstmt.setDate(2, new java.sql.Date(record_date.getTime()));
		cstmt.setString(3, central_No);
		cstmt.setString(4, columnName);
		cstmt.registerOutParameter(5, DB2Types.CURSOR);
		cstmt.registerOutParameter(6, Types.VARCHAR);
		
		cstmt.execute();
		
		int returnCode = cstmt.getInt(1);
		
		if (returnCode != 0) {
			String errorMessage = cstmt.getString(6);
            throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
		}
		
		java.sql.ResultSet rs = (java.sql.ResultSet)cstmt.getObject(5);
		while (rs.next()) {
        	System.out.println(rs.getString(1) + " " + rs.getString(2)); // test
        	
        	String code = rs.getString(1);
        	code = (code == null)?"":code.trim();
        
        	String name = rs.getString(2);
        	name = (name == null)?"":name.trim();
        	
        	resultMap.put(code, name);
		}
        
        System.out.println("Map Size = " + resultMap.size()); // test
        
        // 若Size為0則發出錯誤警告
        if (resultMap.size() == 0) {
//        	System.out.println("Map " + columnName + " 不存在!!");
        	throw new Exception("Map " + columnName + " 不存在!!");
        }
		
		return resultMap;
	}
	
	public Map<String, Map<String, String>> getCheckMaps(Date record_date, String central_No, String[][] checkColumnArray) throws Exception {
		
		for (int i = 0 ; i < checkColumnArray.length; i++) {
			checkMaps.put(checkColumnArray[i][0], getCheckMap(record_date, central_No, checkColumnArray[i][1]));
		}
		
		return checkMaps;
	}

	public static void main(String[] argv) {
		
		try {
			
//			getCheckList("TimTest");
//			getCheckMap("TimTest");
			
			String[][] // checkColumnList = new String[2][];
			checkColumnList = {
					{"PARTY_PHONE_column_1", "TimTest"},
					{"PARTY_PHONE_column_2", "TimTest"}, 
					{"PARTY_PHONE_column_3", "TimTest"}};
			
			ETL_Q_ColumnCheckCodes one = new ETL_Q_ColumnCheckCodes();
			
			Map<String, Map<String, String>> maps = one.getCheckMaps(new Date(), "600", checkColumnList);
			System.out.println("size = " + maps.size());
			Map<String, String> map;
			map = maps.get("PARTY_PHONE_column_1");
			System.out.println("PARTY_PHONE_column_1 test");
			System.out.println("L exists is " + map.containsKey("L"));
			System.out.println("L2 exists is " + map.containsKey("L2"));
			map = maps.get("PARTY_PHONE_column_2");
			System.out.println("PARTY_PHONE_column_2 test");
			System.out.println("L exists is " + map.containsKey("L"));
			System.out.println("L2 exists is " + map.containsKey("L2"));
		
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
	}

}
