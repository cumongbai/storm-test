package storm.kafka;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.redis.common.config.JedisPoolConfig;
import storm.redis.trident.state.RedisMapState;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.state.StateFactory;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 注册日志实时统计topology
 * 1.根据游戏id和广告码分组统计，统计结果写入redis
 * 创建人：philo <br>
 * 创建时间：2015-11-9 <br>
 * 功能描述： <br>
 * 版本： <br>
 * 版权拥有：深圳中青宝互动网络股份有限公司 <br>
 * ====================================== <br>
 *               修改记录 <br>
 * ====================================== <br>
 *  序号    姓名      日期      版本           简单描述 <br>
 *
 */
public class RegisterLogProcesser {

	public static class splitBolt extends BaseFunction{

		private String REG_LOG_REG = "^I \\d{2}\\-\\d{2}_\\d{2}:\\d{2}:\\d{2} GetRegisterUserInfoByActiveMQ-registerUserInfo";
		/**
		 * 
		 */
		private static final long serialVersionUID = 2208916620809309559L;

		public void execute(TridentTuple tuple, TridentCollector collector) {
		      String sentence = tuple.getString(0);
		      Pattern pattern = Pattern.compile(REG_LOG_REG);
		      Matcher matcher = pattern.matcher(sentence);
		      if(matcher.find())
		      {
		    	  String[] arrStrings = sentence.split(",");
		    	  int len = arrStrings.length;		    	  
		    	  if(len > 9){
		    		  String adCode = null;
			    	  String gameId = null;
			    	  for(int i = 0; i < len; i++)
			    	  {
			    	  	if(i == 4){
			    	  		adCode = arrStrings[i];
			    	  	}else if(i == 9){
			    	  		gameId = arrStrings[i];
			    	  		break;
			    	  	}		    	  				    	  	
			    	  }
			    	  if(!gameId.isEmpty() && !adCode.isEmpty()){
				    	  adCode = "reg."+gameId+"."+adCode; 
				    	  System.out.println("fetch register log:{"+adCode+"}");
				    	  collector.emit(new Values(adCode));
			    	  }
		    	  }
		    	  
		      }
		}
		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		/**
		 * kafka队列获取register信息
		 */
		TridentTopology topology = new TridentTopology();
		BrokerHosts zk = new ZkHosts(EnvConfig.ZK_HOST);
		TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, "log_reg");
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

        
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
        .setHost(EnvConfig.REDIS_HOST).setPort(EnvConfig.REDIS_PORT).build();
        StateFactory factory = RedisMapState.nonTransactional(poolConfig);


		/**
		 * 提取register记录，按广告码统计
		 * 
		 */
		Stream stream = topology.newStream("spout_reg", spout).parallelismHint(2);
		stream.each(new Fields("str"),
		        new splitBolt(), new Fields("ad_code")).parallelismHint(2).groupBy(new Fields("ad_code"))
		        .persistentAggregate(factory, new Count(), new Fields("reg_counter")).parallelismHint(2);
				
		Config conf = new Config();
		//设置ack追踪线程为0，取消可靠性，提高性能,本地模式调试时须关闭，否则启动不了
		//conf.setNumAckers(0);
		
		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("register_statics", conf, topology.build());

	}

}
