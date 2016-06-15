package storm.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RedisMapUtils {

	/**
	 * 将如1200:10,1201:11...的历史数据转换为map
	 *  
	 * @param hisDataStr
	 * @return Map<String,Integer>
	 * @author:Administrator 
	 * @date:2015-11-11
	 */
	public static Map<String, Integer> transferToMap(String hisDataStr){
		
		Map<String, Integer> transferedMap = new HashMap<String, Integer>();
		String[] splitedStr = hisDataStr.split(",");
		String[] keyValue = null;
		String timeStrl;
		int staticsCount;
		
		for(String tmpHisData: splitedStr){
			keyValue = tmpHisData.split(":");
			if(keyValue.length >= 2){
				timeStrl = keyValue[0];
				staticsCount = Integer.parseInt(keyValue[1]);
				transferedMap.put(timeStrl, staticsCount);
			}
		}
		return transferedMap;
	}
	
	/**
	 * 将map转换为形如1200:10,1201:11...字符串
	 *  
	 * @param hisDataMap
	 * @return String
	 * @author:Administrator 
	 * @date:2015-11-11
	 */
	public static String transferToString(Map<String, Integer> hisDataMap){
		Set<String> keySet = hisDataMap.keySet();
		StringBuilder strBuilder = new StringBuilder();
		for(String tmpKey : keySet){
			strBuilder.append(tmpKey).append(":").append(hisDataMap.get(tmpKey)).append(",");
		}
		return strBuilder.toString();
	}
	
	/** 
	 *  
	 * @param args void
	 * @author:Administrator 
	 * @date:2015-11-11 
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String x = "1200:10,1201:11,1202:15,";
		Map<String, Integer> tmpMap = transferToMap(x);
		System.out.println(transferToString(tmpMap));
	}

}
