package storm.kafka;

public class AdLogHelper {

	/**
	 * 处理注册日志
	 *  
	 * @param ReglogStr void
	 * @author:Administrator 
	 * @date:2015-11-9
	 */
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
	
	/** 
	 *  
	 * @param args void
	 * @author:Administrator 
	 * @date:2015-11-9 
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
