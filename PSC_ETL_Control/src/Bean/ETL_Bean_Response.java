package Bean;

import java.io.Serializable;
import java.util.List;

public class ETL_Bean_Response implements Serializable {
	private static final long serialVersionUID = 1L;
	private boolean isSuccess;
	private String error;
	private List<?> list;
	private Object obj;
	
	public ETL_Bean_Response() {
		isSuccess = false;
		error = "";
	}
	
	public boolean isSuccess() {
		return isSuccess;
	}
	public void setSuccess(boolean isSuccess) {
		this.isSuccess = isSuccess;
	}
	public String getError() {
		return error;
	}
	public void setError(String error) {
		this.error = error;
	}
	public List<?> getList() {
		return list;
	}
	public void setList(List<?> list) {
		this.list = list;
	}
	public Object getObj() {
		return obj;
	}
	public void setObj(Object obj) {
		this.obj = obj;
	}
	public void setErrorReason(String error){
		this.isSuccess = false;
		this.error = error;
	}
	
	public void setSuccessObj(Object obj){
		this.isSuccess = true;
		this.obj = obj;
	}
	public void setSuccessObjList(List<?> list){
		this.isSuccess = true;
		this.list = list;
	}
	
}
