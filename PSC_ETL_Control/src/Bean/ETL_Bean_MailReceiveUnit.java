package Bean;

public class ETL_Bean_MailReceiveUnit {
	
	private String mail_type; // 收件者mail類型
	private String type_value; // 收件者類型值(單位)
	
	public ETL_Bean_MailReceiveUnit(String mail_type, String type_value) {
		this.mail_type = mail_type;
		this.type_value = type_value;
	}
	
	public String getMail_type() {
		return mail_type;
	}
	public void setMail_type(String mail_type) {
		this.mail_type = mail_type;
	}
	public String getType_value() {
		return type_value;
	}
	public void setType_value(String type_value) {
		this.type_value = type_value;
	}

}
