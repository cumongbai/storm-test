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
import storm.trident.operation.builtin.Sum;
import storm.trident.state.StateFactory;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 支付日志topology
 * 1.按广告码统计支付总金额
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
public class PayLogProcessor {

	// 支付日志正则
	private static String PAY_LOG_REG = "^I \\d{2}\\-\\d{2}_\\d{2}:\\d{2}:\\d{2} GetPayRecordsByActiveMQ-end 200 \"\\{REQ_PARAMS_MAP=ACCOUNTS=[^&]+&GAME_ID=(\\d+)&AD_CODE=(\\w+)&RE_AMOUNT=(\\d+)&[\\w\\W]+";

	public static class splitBolt extends BaseFunction {		

		/**
		 * 
		 */
		private static final long serialVersionUID = 4263405715198112288L;

		public void execute(TridentTuple tuple, TridentCollector collector) {
			String sentence = tuple.getString(0);
			Pattern pattern = Pattern.compile(PAY_LOG_REG);
			Matcher matcher = pattern.matcher(sentence);
			if (matcher.find()) {
				String gameId = matcher.group(1);
				String adCode = matcher.group(2);
				String amount = matcher.group(3);

				if (!gameId.isEmpty()) {
					adCode = "pay."+gameId+"."+adCode;
					int payAmount = Integer.parseInt(amount);
					collector.emit(new Values(adCode, payAmount));
					System.out.println("fetch pay log:{" + adCode + "," + amount + "}");
				}			

			}

		}

	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		/**
		 * kafka队列获取pay信息
		 */
		TridentTopology topology = new TridentTopology();
		BrokerHosts zk = new ZkHosts(EnvConfig.ZK_HOST);
		TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, "log_pay");
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

        
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
        .setHost(EnvConfig.REDIS_HOST).setPort(EnvConfig.REDIS_PORT).build();
        StateFactory factory = RedisMapState.nonTransactional(poolConfig);


		/**
		 * 提取pay记录，按广告码统计总金额
		 * 
		 */
		Stream stream = topology.newStream("spout_pay", spout).parallelismHint(2);
		stream.each(new Fields("str"),
		        new splitBolt(), new Fields("ad_code","amount")).parallelismHint(2).groupBy(new Fields("ad_code"))
		        .persistentAggregate(factory, new Fields("amount"), new Sum(), new Fields("pay_sum")).parallelismHint(2);
				
		Config conf = new Config();
		//设置ack追踪线程为0，取消可靠性，提高性能,本地模式调试时须关闭，否则启动不了
		//conf.setNumAckers(0);
		
		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("pay_statics", conf, topology.build());
	}

}
