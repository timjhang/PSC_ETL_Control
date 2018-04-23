package Bean;

import java.util.HashMap;

public class ETL_Bean_CLP_Script {
	
	private HashMap<Integer,String> map; 
	private String modelFilePath;
	private String destinationFilePath;
	
	
	public HashMap<Integer, String> getMap() {
		return map;
	}
	
	public void setMap(HashMap<Integer, String> map) {
		this.map = map;
	}
	
	public String getModelFilePath() {
		return modelFilePath;
	}
	
	public void setModelFilePath(String modelFilePath) {
		this.modelFilePath = modelFilePath;
	}
	
	public String getDestinationFilePath() {
		return destinationFilePath;
	}
	
	public void setDestinationFilePath(String destinationFilePath) {
		this.destinationFilePath = destinationFilePath;
	}
	
}
