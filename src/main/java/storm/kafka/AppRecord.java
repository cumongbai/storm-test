package storm.kafka;

import java.io.Serializable;

public class AppRecord implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -189002227013357043L;
	private String APP_ID;
	private String IMEI;
	private String MAC;
	private String IP_ADDRESS;
	private String FIRST_FLAG;
	private String EVENT_TYPE;
	private String OPT_TYPE;
	private String OPT_TIME;
	private String AD_CODE;
	
	
	public String getAPP_ID() {
		return APP_ID;
	}


	public void setAPP_ID(String aPP_ID) {
		APP_ID = aPP_ID;
	}


	public String getIMEI() {
		return IMEI;
	}


	public void setIMEI(String iMEI) {
		IMEI = iMEI;
	}


	public String getMAC() {
		return MAC;
	}


	public void setMAC(String mAC) {
		MAC = mAC;
	}


	public String getIP_ADDRESS() {
		return IP_ADDRESS;
	}


	public void setIP_ADDRESS(String iP_ADDRESS) {
		IP_ADDRESS = iP_ADDRESS;
	}


	public String getFIRST_FLAG() {
		return FIRST_FLAG;
	}


	public void setFIRST_FLAG(String fIRST_FLAG) {
		FIRST_FLAG = fIRST_FLAG;
	}


	public String getEVENT_TYPE() {
		return EVENT_TYPE;
	}


	public void setEVENT_TYPE(String eVENT_TYPE) {
		EVENT_TYPE = eVENT_TYPE;
	}


	public String getOPT_TYPE() {
		return OPT_TYPE;
	}


	public void setOPT_TYPE(String oPT_TYPE) {
		OPT_TYPE = oPT_TYPE;
	}


	public String getOPT_TIME() {
		return OPT_TIME;
	}


	public void setOPT_TIME(String oPT_TIME) {
		OPT_TIME = oPT_TIME;
	}


	public String getAD_CODE() {
		return AD_CODE;
	}


	public void setAD_CODE(String aD_CODE) {
		AD_CODE = aD_CODE;
	}


	/** 
	 *  
	 * @param args void
	 * @author:Administrator 
	 * @date:2015-10-21 
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
