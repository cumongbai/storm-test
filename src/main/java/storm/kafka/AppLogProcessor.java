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

public class AppLogProcessor {

	private static String APP_LOG_REG = "^I \\d{2}\\-\\d{2}_\\d{2}:\\d{2}:\\d{2} GetAppRecordsByActiveMQ-end 200 \"\\{REQ_PARAMS_MAP=IMEI=[^&]+&MAC=[^&]+&IP_ADDRESS=[\\.\\w]{7,15}+&FIRST_FLAG=(\\d)&EVENT_TYPE=(\\d)&OPT_TYPE=\\d&OPT_TIME=\\d{4}\\-\\d{2}\\-\\d{2}[%\\w]+&AD_CODE=(\\w+), APP_ID=(\\w+), [\\w\\W]+";

	public static class splitBolt extends BaseFunction {
		/**
		 * 
		 */
		private static final long serialVersionUID = 2355798794852394315L;

		public void execute(TridentTuple tuple, TridentCollector collector) {
			String sentence = tuple.getString(0);

			Pattern pattern = Pattern.compile(APP_LOG_REG);
			Matcher matcher = pattern.matcher(sentence);
			if (matcher.find()) {

				String firstFlag = matcher.group(1);
				String eventType = matcher.group(2);
				String adCode = matcher.group(3);
				String appId = matcher.group(4);
				if(!appId.isEmpty() && !adCode.isEmpty() && !"null".equalsIgnoreCase(adCode))
				{
					adCode = "app." + appId + "." + matcher.group(3) + "." + firstFlag + "-" + eventType;
	
					System.out.println("fetch app log:{" + adCode + "}");
	
					collector.emit(new Values(adCode));
				}
			}

		}

	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		TridentTopology topology = new TridentTopology();
		BrokerHosts zk = new ZkHosts(EnvConfig.ZK_HOST);
		TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, "log_app");
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);


		JedisPoolConfig poolConfig = new JedisPoolConfig.Builder().setHost(EnvConfig.REDIS_HOST).setPort(EnvConfig.REDIS_PORT).build();
		StateFactory factory = RedisMapState.nonTransactional(poolConfig);

		/**
		 * 提取app记录，按广告码统计 todo: 1.添加统计时间点，便于分析 展示程序按特定时间间隔(如2分钟)去获取统计数据，保存
		 * 优点：数据生产和展示分离，缓存数据量最少，统计时间单位由展示程序控制 缺点：非完全实时，需要定时任务去获取所有相关缓存数据。
		 * 2.当天数据清零 所有数据显示时减去当天凌晨0点0分的数据
		 * 
		 */
		Stream stream = topology.newStream("spout_app", spout).parallelismHint(1);
		stream.each(new Fields("str"), new splitBolt(), new Fields("ad_code")).parallelismHint(2).groupBy(new Fields("ad_code")).persistentAggregate(factory, new Count(), new Fields("count"))
				.parallelismHint(2);

		Config conf = new Config();
		// 设置ack追踪线程为0，取消可靠性，提高性能,本地模式调试时须关闭，否则启动不了
		// conf.setNumAckers(0);

		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("app_statics", conf, topology.build());
	}

}
