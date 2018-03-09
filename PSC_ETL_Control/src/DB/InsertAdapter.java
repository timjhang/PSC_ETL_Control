package DB;

import java.util.List;

public class InsertAdapter {
	// insert DB2輔助class
	
	// 傳至DB2array限制長度
	private int typeArrayLength;  
	// 執行SQL語句
	private String sql;
	// DB2 type名稱
	private String createStructTypeName;
	// DB2 Array type名稱
	private String createArrayTypesName;
	
	public int getTypeArrayLength() {
		return typeArrayLength;
	}
	
	public void setTypeArrayLength(int typeArrayLength) {
		this.typeArrayLength = typeArrayLength;
	}
	
	public String getSql() {
		return sql;
	}
	
	public void setSql(String sql) {
		this.sql = sql;
	}
	
	public String getCreateStructTypeName() {
		return createStructTypeName;
	}
	
	public void setCreateStructTypeName(String createStructTypeName) {
		this.createStructTypeName = createStructTypeName;
	}
	
	public String getCreateArrayTypesName() {
		return createArrayTypesName;
	}
	
	public void setCreateArrayTypesName(String createArrayTypesName) {
		this.createArrayTypesName = createArrayTypesName;
	}
	
}
