package storm.kafka;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;


/**
 * 获取某一时刻缓存中的数据
 * 创建人：philo <br>
 * 创建时间：2015-11-11 <br>
 * 功能描述： <br>
 * 版本： <br>
 * 版权拥有：深圳中青宝互动网络股份有限公司 <br>
 * ====================================== <br>
 *               修改记录 <br>
 * ====================================== <br>
 *  序号    姓名      日期      版本           简单描述 <br>
 *
 */
public class DataSnapshot implements Runnable{

	//要处理的缓存数据前辍，包括app，注册，支付数据
	private static String[] prefixes = {"app","reg","pay"};
	public static AtomicInteger timeCounter;
	private int localCacheSize = 1000;
	private String globalKey = "$REDIS-MAP-STATE-GLOBAL";
	private int connectionTimeout = Protocol.DEFAULT_TIMEOUT;
	private String password = null;
	private int database = Protocol.DEFAULT_DATABASE;
	private String host = "192.168.111.128";
	private int port = 6379;
    
	JedisPool pool = new JedisPool(new JedisPoolConfig(),
            host, port, connectionTimeout, password, database);
	
	
	/** 
	 *  
	 * @param args void
	 * @author:Administrator 
	 * @date:2015-11-5 
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		DataSnapshot redisHelper = new DataSnapshot();
		for(String prefix : prefixes){
		    redisHelper.statics(prefix);
		}
		redisHelper.shootShow();
	}
	
	/**
	 * 批量获取redis存储的数据
	 *  
	 * @param prefix
	 * @return Map<String,String>
	 * @author:philo
	 * @date:2015-11-10
	 */
	private Map<String, String> patternGetStatics(String prefix) {
	      Jedis jedis = pool.getResource();
	      try {
	    	  Set<String> keys = jedis.keys(prefix);
	    	  if(keys.isEmpty())
	    	  {
	    		  return null;
	    	  }
	    	  
	    	  String[] names = new String[keys.size()];
	    	  keys.toArray(names);
	    	  List<String> countList = jedis.mget(names);
	    	  Map<String, String> resultMap = new HashMap<>();
	    	  for(int i =0; i<names.length; i++)
	    	  {
	    		  resultMap.put(names[i], countList.get(i));
	    	  }
	    	  return resultMap;

	      } finally {
	         pool.returnResource(jedis);
	      }
	   }
	

	/**
	 * 批量获取缓存数据
	 *  
	 * @param keys
	 * @return List<String>
	 * @author:philo
	 * @date:2015-11-10
	 */
	private List<String> mget(String... keys) {
	      Jedis jedis = pool.getResource();
	      try {
	         return jedis.mget(keys);
	      } finally {
	         pool.returnResource(jedis);
	      }
	   }
	
	/**
	 * 批量保存缓存数据
	 *  
	 * @param keyValues void
	 * @author:philo 
	 * @date:2015-11-10
	 */
	private void mset(String... keyValues) {
	    Jedis jedis = pool.getResource();
	    try {
	       jedis.mset(keyValues);
	    } finally {
	       pool.returnResource(jedis);
	    }
	 }
	
	/**
	 * 抓取某个时间点缓存的数据，配合定时任务可以制作出广告码随时间变化曲线
	 * 如果以1分钟的时间粒度，5000个广告码一天产生720*5000个数据
	 * 格式：app_his.<广告码>:{1201:100,1202:102...}
	 * 
	 *   void
	 * @author:Administrator 
	 * @date:2015-11-6
	 */
	public void statics(String prefix)
	{
		long time1 = System.currentTimeMillis();
		//当前数据
		String realTimeDataPattern = prefix+".*";
		//当天历史数据
		String hisDataPattern = prefix+"_his.*";
		
		Map<String, String> appData = patternGetStatics(realTimeDataPattern);
		if(appData == null)
		{
			return;
		}
		Map<String, String> hisAppData = patternGetStatics(hisDataPattern);
		if(hisAppData == null){
			hisAppData = new HashMap<>();
		}
		
		//计算当前数据要保存的key值，计数器当前累加值即数据快照
		DateFormat df = new SimpleDateFormat("HHmm");
        
		String timeString = df.format(new Date());
				
		//当前时间点广告码分组对应的统计数目
		int staticsCount;		
		//历史app统计数据key值
		String hisDataKey;
		Set<String> appDataKeys = appData.keySet();
		
		//历史app统计数据反序列化value值
		Map<String, Integer> hisAppDataValue;
		//历史app统计数据序列化value值
		String tmpHisAppDataValue;
		int counter = 0;
		
		
		String[] currKeyValues = new String[appDataKeys.size()*2];
		for(String tmpAppDataKey: appDataKeys)
		{
			staticsCount = Integer.parseInt(appData.get(tmpAppDataKey));
			hisDataKey = tmpAppDataKey.replace(prefix+".", prefix+"_his.");
			
			tmpHisAppDataValue = hisAppData.get(hisDataKey);
			//已在在，添加到历史数据map中，
			if(tmpHisAppDataValue != null){
				hisAppDataValue = RedisMapUtils.transferToMap(tmpHisAppDataValue);
				hisAppDataValue.put(timeString, staticsCount);
			}else {
				hisAppDataValue = new HashMap<>();
				hisAppDataValue.put(timeString, staticsCount);		
			}
					
			//历史数据key，类似app_his.20130716173850ou8iWv.750001843409.1-1
			currKeyValues[counter++] = hisDataKey;
			//历史数据value,{1201:10,...}
			currKeyValues[counter++] = RedisMapUtils.transferToString(hisAppDataValue);
			
		}
		
		mset(currKeyValues);
		
		System.out.println("总体消耗时间："+(System.currentTimeMillis() - time1));
		
	}

	public void shootShow()
	{
		
		Map<String, String> hisAppData = patternGetStatics("app_his.*");
		Set<String> appDataKeys = hisAppData.keySet();
		Map<String, Integer> hisAppDataValue;
		for(String tmpAppDataKey: appDataKeys)
		{
			hisAppDataValue = RedisMapUtils.transferToMap(hisAppData.get(tmpAppDataKey));
			System.out.println(tmpAppDataKey);
			Set<String> keys = hisAppDataValue.keySet();
			for(String key:keys)
			{
				System.out.println(key+":"+hisAppDataValue.get(key));
			}
			
		}
	}
	
	@Override
	public void run() {
		for(String prefix : prefixes){
		    statics(prefix);
		}
	}	
	
}


