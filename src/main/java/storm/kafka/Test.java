package storm.kafka;

import java.util.Date;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

public class Test {

	protected String sidx;
    protected String sord;
    private Long id =1l;
    private String ad_code ="111";
    private String location_domain ="2";
    private String location_url ="3";
    private String titile="4";
    private String referrer_url="5";
    private String referrer_domain="6";
    private String lang="7";
    private String cookie_enabled="8";
    private String cookie_flag="9";
    private String ip="10";
    private int stat_type=11;
    private int click_type=12;
    private String keyword="13";
    private Long timestamp=14L;
    private String game_id="15";
    private Date created_at=new Date();
    private Date updated_at=new Date();
	/** 
	 *  
	 * @param args void
	 * @author:Administrator 
	 * @date:2015-11-6 
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Test test = new Test();
		/*System.out.println(test);
		String tmp = "I 11-09_08:59:58 VisitRecordsService-onProcess 200 \"<null>,,zqgame.com,http://ff.zqgame.com/,《最后一炮》官网 - FF - 中国现代装甲网游精品 - 今日开测,https://www.baidu.com/link?url=p8J1vN3s3EnSIFbO7D1MunmGZPwL6kgFHddj4XS5NGa&wd=&eqid=aca1a3870000fe5800000005563fec79,www.baidu.com,zh-CN,true,GnzrTVckd3YYYY1109084848,220.166.199.249, 10.119.4.14,3,0,,694176,86,Mon Nov 09 08:59:58 CST 2015,<null>,<null>,<null>\" \"\" \"\"";
		String[] arrStrings = tmp.split(",");
		int len = arrStrings.length;
		int count = 0;
		for(int i = len -1; i > 0; i--)
		{
			count++;
			if(count == 8){
				System.out.println("click_type:"+arrStrings[i]);
			}
			else if(count == 9){
				System.out.println("stat_type:"+arrStrings[i]);
				break;
			}
			//System.out.println(arrStrings[i]);
		}*/
		String regLog = "I 11-09_14:07:51 GetRegisterUserInfoByActiveMQ-registerUserInfo 200 \"<null>,15854430791,菜鸟,211111111111111112,15265,,221.1.104.242,574,0,86,rw_ff,<null>,<null>,Mon Nov 09 14:04:10 CST 2015,Mon Nov 09 14:07:51 CST 2015,<null>,<null>,<null>,<null>,<null>\" \"\" \"\"";
		test.processRegister(regLog);
	}
	
	public void processAdvert(String adLogStr){
		String[] arrStrings = adLogStr.split(",");
		int len = arrStrings.length;
		int count = 0;
		for(int i = len -1; i > 0; i--)
		{
			count++;
			if(count == 8){
				System.out.println("click_type:"+arrStrings[i]);
			}
			else if(count == 9){
				System.out.println("stat_type:"+arrStrings[i]);
				break;
			}
			//System.out.println(arrStrings[i]);
		}
	}
	
	public void processRegister(String ReglogStr){
		String[] arrStrings = ReglogStr.split(",");
		int len = arrStrings.length;
		for(int i = 0; i < len; i++)
		{
			if(i == 4){
				System.out.println("ad_code_id:"+arrStrings[i]);
			}else if(i == 9){
				System.out.println("game_id:"+arrStrings[i]);
				break;
			}
			
			
		}
	}
	
	public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SIMPLE_STYLE);
    }

	public String getSidx() {
		return sidx;
	}

	public void setSidx(String sidx) {
		this.sidx = sidx;
	}

	public String getSord() {
		return sord;
	}

	public void setSord(String sord) {
		this.sord = sord;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getAd_code() {
		return ad_code;
	}

	public void setAd_code(String ad_code) {
		this.ad_code = ad_code;
	}

	public String getLocation_domain() {
		return location_domain;
	}

	public void setLocation_domain(String location_domain) {
		this.location_domain = location_domain;
	}

	public String getLocation_url() {
		return location_url;
	}

	public void setLocation_url(String location_url) {
		this.location_url = location_url;
	}

	public String getTitile() {
		return titile;
	}

	public void setTitile(String titile) {
		this.titile = titile;
	}

	public String getReferrer_url() {
		return referrer_url;
	}

	public void setReferrer_url(String referrer_url) {
		this.referrer_url = referrer_url;
	}

	public String getReferrer_domain() {
		return referrer_domain;
	}

	public void setReferrer_domain(String referrer_domain) {
		this.referrer_domain = referrer_domain;
	}

	public String getLang() {
		return lang;
	}

	public void setLang(String lang) {
		this.lang = lang;
	}

	public String getCookie_enabled() {
		return cookie_enabled;
	}

	public void setCookie_enabled(String cookie_enabled) {
		this.cookie_enabled = cookie_enabled;
	}

	public String getCookie_flag() {
		return cookie_flag;
	}

	public void setCookie_flag(String cookie_flag) {
		this.cookie_flag = cookie_flag;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getStat_type() {
		return stat_type;
	}

	public void setStat_type(int stat_type) {
		this.stat_type = stat_type;
	}

	public int getClick_type() {
		return click_type;
	}

	public void setClick_type(int click_type) {
		this.click_type = click_type;
	}

	public String getKeyword() {
		return keyword;
	}

	public void setKeyword(String keyword) {
		this.keyword = keyword;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public String getGame_id() {
		return game_id;
	}

	public void setGame_id(String game_id) {
		this.game_id = game_id;
	}

	public Date getCreated_at() {
		return created_at;
	}

	public void setCreated_at(Date created_at) {
		this.created_at = created_at;
	}

	public Date getUpdated_at() {
		return updated_at;
	}

	public void setUpdated_at(Date updated_at) {
		this.updated_at = updated_at;
	}

	
}
