package storm.kafka;

import java.io.Serializable;

/**
 * app统计数据显示
 * 创建人：philo <br>
 * 创建时间：2015-11-6 <br>
 * 功能描述： <br>
 * 版本： <br>
 * 版权拥有：深圳中青宝互动网络股份有限公司 <br>
 * ====================================== <br>
 *               修改记录 <br>
 * ====================================== <br>
 *  序号    姓名      日期      版本           简单描述 <br>
 *
 */
public class AppChartData implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 4292476569865379797L;
	private String adCode;
	//统计数据，以分钟为单位，格式为{hhmm:数目}
	private String[] statDatam = new String[720];
	
	
	public String getAdCode() {
		return adCode;
	}


	public void setAdCode(String adCode) {
		this.adCode = adCode;
	}


	public String[] getStatDatam() {
		return statDatam;
	}


	public void setStatDatam(String[] statDatam) {
		this.statDatam = statDatam;
	}

}
