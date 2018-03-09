package DB;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	public static Map<String, String> getCheckMap(String columnName) throws Exception {
		
		// Code & Name
		Map<String, String> resultMap = new HashMap<String, String>();
		
		Connection con = ConnectionHelper.getDB2Connection();
		
		java.sql.Statement stmt = con.createStatement();
		String query = "SELECT CODE, NAME FROM " + ETL_Profile.db2TableSchema + ".CODEPOOL "
				+ "WHERE TABLESPACENAME = \'" + columnName + "\'";
        java.sql.ResultSet rs = stmt.executeQuery(query);
        
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
	
	public Map<String, Map<String, String>> getCheckMaps(String[][] checkColumnArray) throws Exception {
		
		for (int i = 0 ; i < checkColumnArray.length; i++) {
			checkMaps.put(checkColumnArray[i][0], getCheckMap(checkColumnArray[i][1]));
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
			
			Map<String, Map<String, String>> maps = one.getCheckMaps(checkColumnList);
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
