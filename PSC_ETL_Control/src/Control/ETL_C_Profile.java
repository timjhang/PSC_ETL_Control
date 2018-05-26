package Control;

import Tool.ETL_Tool_DES;

public class ETL_C_Profile {

	// GAML Server下載Master及壓縮檔路徑
	public static final String ETL_Download_localPath = ETL_Tool_DES.decrypt("decbb834578d510a250db3230e803611");
	// ETL Server下載Master及壓縮檔路徑
	public static final String ETL_Download_RemotePath = ETL_Tool_DES.decrypt("de6964a598384312250db3230e803611");
	
	public static final String sftp_hostName = ETL_Tool_DES.decrypt("bcebee3b8e6d86fd83ac7b2afbd08863");
	public static final String sftp_port = ETL_Tool_DES.decrypt("319aa7094b01f869");
	public static final String sftp_username = ETL_Tool_DES.decrypt("0f62db871d1d0101");
	public static final String sftp_password = ETL_Tool_DES.decrypt("76acb6643a82091ac1a9d09a87ddb35b");
	
}
