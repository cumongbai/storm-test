package storm.kafka;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import storm.redis.common.config.JedisPoolConfig;


import storm.redis.common.config.JedisClusterConfig;
import storm.redis.common.mapper.RedisDataTypeDescription;
import storm.redis.trident.state.RedisClusterMapState;
import storm.redis.trident.state.RedisMapState;



import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.MapGet;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.mysql.MysqlStateConfig;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 
 * 创建人：Administrator <br>
 * 创建时间：2015-9-22 <br>
 * 功能描述： storm与kafka队列集成
 * 1.服务器上需要配置hosts,添加kafka集群的各机器名，否则会报ClosedChannelException<br>
 * 版本： <br>
 * 版权拥有：深圳中青宝互动网络股份有限公司 <br>
 * ====================================== <br>
 *               修改记录 <br>
 * ====================================== <br>
 *  序号    姓名      日期      版本           简单描述 <br>
 *
 */
public class KafkaTest {

	/**
	 * app事件处理完成日志正则表达式
	 * 只获取广告码在在的记录
	 */
	private static String APP_LOG_REG = "I \\d{2}\\-\\d{2}_\\d{2}:\\d{2}:\\d{2} GetAppRecordsByActiveMQ-end 200 \"\\{REQ_PARAMS_MAP=IMEI=[\\-\\w]+&MAC=[\\-:\\w]+&IP_ADDRESS=[\\.\\w]{7,15}+&FIRST_FLAG=(\\d)&EVENT_TYPE=(\\d)&OPT_TYPE=(\\d)&OPT_TIME=\\d{4}\\-\\d{2}\\-\\d{2}[%\\w]+&AD_CODE=(\\w+), APP_ID=(\\w+), [\\w\\W]+";
	
	private static String PAY_LOG_REG = "I \\d{2}\\-\\d{2}_\\d{2}:\\d{2}:\\d{2} GetPayRecordsByActiveMQ-end 200 \"\\{REQ_PARAMS_MAP=ACCOUNTS=(\\w+)&GAME_ID=(\\d+)&AD_CODE=(\\w+)&RE_AMOUNT=(\\d+)&ORDER_ID=(\\w+),[\\w\\W]+";
	
	public static class ExclamationBolt extends BaseBasicBolt {

	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	      declarer.declare(new Fields("out_str"));
	    }

	    public void execute(Tuple tuple, BasicOutputCollector collector) {
	      String arg = tuple.getString(0);
	      System.out.println(arg+"---");
	      collector.emit(new Values(arg + "!"));
	    }

	  }
	
	public static class PrintOut extends BaseFunction {
	    public void execute(TridentTuple tuple, TridentCollector collector) {
	      String sentence = tuple.getString(0);
	      System.out.println(sentence+"---"+tuple.getStringByField("str"));
	    }
	  }
	
	/**
	 * 处理日志
	 * 创建人：zhangzhenhua <br>
	 * 创建时间：2015-10-21 <br>
	 * 功能描述： <br>
	 * 版本： <br>
	 * 版权拥有：深圳中青宝互动网络股份有限公司 <br>
	 * ====================================== <br>
	 *               修改记录 <br>
	 * ====================================== <br>
	 *  序号    姓名      日期      版本           简单描述 <br>
	 *
	 */
	public static class splitBolt extends BaseFunction{
		/**
		 * 
		 */
		private static final long serialVersionUID = 2355798794852394315L;

		public void execute(TridentTuple tuple, TridentCollector collector) {
		      String sentence = tuple.getString(0);
		      if(sentence.length() > 60)
		      {
		    	  sentence = sentence.substring(17);
		    	  if(sentence.startsWith("GetAppRecordsByActiveMQ-start"))
		    	  {
		    		  int startPoint = sentence.indexOf("{")+1;
		    		  int endPoint = sentence.indexOf("}", startPoint);
		    		  String paramString = sentence.substring(startPoint, endPoint);
		    		  String[] params = paramString.split(", ");
		    		  if(params[1].startsWith("APP_ID="))
		    		  {
		    			  collector.emit(new Values(params[1].trim()+"&"+params[0].trim().substring(15)));
		    			  System.out.println("split bolt process completed.");
		    		  }
		    		  
		    	  }
		    	  else {
					System.out.println("Other logs, ignored.");
				}
		      }
		}
	}
	
	/**
	 * 解析app事件日志，提取app_id,ad_code,事件类型信息
	 * 创建人：philo <br>
	 * 创建时间：2015-11-3 <br>
	 * 功能描述： <br>
	 * 版本： <br>
	 * 版权拥有：深圳中青宝互动网络股份有限公司 <br>
	 * ====================================== <br>
	 *               修改记录 <br>
	 * ====================================== <br>
	 *  序号    姓名      日期      版本           简单描述 <br>
	 *
	 */
	public static class splitBolt2 extends BaseFunction{
		/**
		 * 
		 */
		private static final long serialVersionUID = 2355798794852394315L;

		public void execute(TridentTuple tuple, TridentCollector collector) {
		      String sentence = tuple.getString(0);
		      if(sentence.length() > 200)
		      {
		    	  Pattern pattern = Pattern.compile(APP_LOG_REG);
		    	  Matcher matcher = pattern.matcher(sentence);
		    	  if(matcher.find()){
			    	  int matchCount = matcher.groupCount();
			    	  if(matchCount >= 5){
			    		  
			    		  String firstFlag = matcher.group(1);
			    		  String eventType = matcher.group(2);
			    		  String optType = matcher.group(3);
			    		  String appId = matcher.group(5);
			    		  String adCode = "app_stat."+appId+"."+matcher.group(4);
			    		  
			    		  System.out.println("fetch app log:{"+appId+","+adCode+","+firstFlag+","+eventType+","+optType+"}");
			    		  
			    		  collector.emit(new Values(appId,adCode,firstFlag,eventType,optType));
			    	  }
		    	  }
		    	  else {
					System.out.println("Other logs, ignored.");
				}
		      }
		}
		
		 /*public void declareOutputFields(OutputFieldsDeclarer declarer) {
		      declarer.declare(new Fields("app_id","ad_code","first_flag","event_type","opt_type"));
		 }*/
	}
	
	public static class RedisBolt extends BaseFunction{

		@Override
		public void execute(TridentTuple tridenttuple, TridentCollector tridentcollector) {
			
			
		}
		
	}
	
	public static class analysisBolt extends BaseFunction{

		public void execute(TridentTuple tuple, TridentCollector collector) {
			String[] params = tuple.getString(0).split("&");
			if(params.length >=9 ){
	
				Method method = null;
				String[] keyValue = null;
				AppRecord appRecord = new AppRecord();
				try {
					for(String tmpStr : params)
					{
						keyValue = tmpStr.split("=");						
						method = AppRecord.class.getMethod("set"+keyValue[0], String.class);
						method.invoke(appRecord, keyValue[1]);
					}
					
					
				} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				System.out.println(appRecord.getAPP_ID()+"---"+appRecord.getAD_CODE()+"---"+appRecord.getEVENT_TYPE());
			}
		}
		
	}
	
	public static class PrintFunction extends BaseFunction{

		@Override
		public void execute(TridentTuple arg0, TridentCollector arg1) {
			// TODO Auto-generated method stub
			System.out.println(arg0.getStringByField("code")+"---"+arg0.getIntegerByField("sum"));
		}
		
	}
	public static class One implements CombinerAggregator<Integer> {
	    public Integer init(TridentTuple tuple) {
	      return 1;
	    }

	    public Integer combine(Integer val1, Integer val2) {
	      return 1;
	    }

	    public Integer zero() {
	      return 1;
	    }
	  }
	/** 
	 *  
	 * @param args void
	 * @author:Administrator 
	 * @throws InvalidTopologyException 
	 * @throws AlreadyAliveException 
	 * @date:2015-9-21 
	 */
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		// TODO Auto-generated method stub

		/*TopologyBuilder builder = new TopologyBuilder();
		BrokerHosts hosts = new ZkHosts("192.168.118.128:2181");
		//zkRoot可设置为日志根目录，id自定义
		SpoutConfig spoutConfig = new SpoutConfig(hosts, "kafka-test", "/tmp/kafka-logs/kafka-test-0", "partition_0");
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);*/
		
		TridentTopology topology = new TridentTopology();
		BrokerHosts zk = new ZkHosts("192.168.111.128");
		TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, "kafka-test");
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);
		/*topology.newStream("spout1", spout).parallelismHint(1).each(new Fields("str"),
		        new splitBolt(), new Fields("word")).parallelismHint(2).each(new Fields("word"),
				        new analysisBolt(), new Fields("appRecord")).parallelismHint(2);*/
		
		/**
		 * mysql
		 */
		final String dburl = "jdbc:mysql://127.0.0.1:3306/storm_test?user=root&password=123456";
		final MysqlStateConfig config = new MysqlStateConfig();
		{
			config.setUrl(dburl);
			config.setTable("advert");
			config.setKeyColumns(new String[]{"app_id","ad_code"});
			config.setValueColumns(new String[]{"count"});
			config.setType(StateType.NON_TRANSACTIONAL);
			config.setCacheSize(5);
		}
		
		/**
		 * redis
		 */
		String host = "192.168.111.128";
        int port = 6379;
		/*JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
        .setHost(host).setPort(port)
        .build();
		RedisStoreMapper storeMapper = new AppStoreMapper();
		RedisLookupMapper lookupMapper = new AppLookupMapper();
		RedisState.Factory factory = new RedisState.Factory(poolConfig); */ 
        
        /*Set<InetSocketAddress> nodes = new HashSet<InetSocketAddress>();
        for (String hostPort : host.split(",")) {
            String[] host_port = hostPort.split(":");
            nodes.add(new InetSocketAddress(host_port[0], Integer.valueOf(host_port[1])));
        }
        JedisClusterConfig clusterConfig = new JedisClusterConfig.Builder().setNodes(nodes)
                                        .build();
        
        RedisDataTypeDescription dataTypeDescription = new RedisDataTypeDescription(
                RedisDataTypeDescription.RedisDataType.HASH, "app");*/
        //StateFactory factory = RedisClusterMapState.nonTransactional(clusterConfig, dataTypeDescription);
        
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
        .setHost(host).setPort(port).build();
        StateFactory factory = RedisMapState.nonTransactional(poolConfig);
        
		Fields fields = new Fields("ad_code");

		/**
		 * 提取app记录，按广告码统计
		 * todo:
		 * 1.添加统计时间点，便于分析
		 * 展示程序按特定时间间隔(如2分钟)去获取统计数据，保存
		 * 优点：数据生产和展示分离，缓存数据量最少，统计时间单位由展示程序控制
		 * 缺点：非完全实时，需要定时任务去获取所有相关缓存数据。
		 * 2.当天数据清零
		 * 所有数据显示时减去当天凌晨0点0分的数据
		 * 
		 */
		Stream stream = topology.newStream("spout1", spout).parallelismHint(1);
		TridentState state = stream.each(new Fields("str"),
		        new splitBolt2(), new Fields("app_id","ad_code","first_flag","event_type","opt_type")).parallelismHint(2).groupBy(new Fields("ad_code"))
		        .persistentAggregate(factory, new Count(), new Fields("sum")).parallelismHint(2);
		
		
		
		        /*.persistentAggregate(factory,
                                fields,
                                new Count(),
                                new Fields("count"));*/
		        //.persistentAggregate(MysqlState.newFactory(config), new Fields("app_id","ad_code"), new Count(), new Fields("sum"));
	
		 /*TridentState wordCounts = topology.newStream("spout1", spout).parallelismHint(6).each(new Fields("sentence"),
			        new Split(), new Fields("word")).groupBy(new Fields("word")).persistentAggregate
			        (new MemoryMapState.Factory(), new Count(), new Fields("count")).parallelismHint(6);*/
		
		Config conf = new Config();
		//设置ack追踪线程为0，取消可靠性，提高性能,本地模式调试时须关闭，否则启动不了
		//conf.setNumAckers(0);
		
		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, topology.build());

		//StormSubmitter.submitTopology("kafka-01", new Config(), topology.build());
		
		// set Spout.
		/*builder.setSpout("word", kafkaSpout);
		builder.setBolt("result", new ExclamationBolt(), 1).shuffleGrouping("word");
		Config conf = new Config();
		conf.setDebug(true);*/

		/*LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		/*conf.setNumWorkers(2);
	    StormSubmitter.submitTopology("kafka-01", conf, builder.createTopology());*/
	}

}
