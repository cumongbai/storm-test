package storm.rabbitmq.test;


import com.rabbitmq.client.ConnectionFactory;

import storm.rabbitmq.MessageScheme;
import storm.rabbitmq.RabbitMQBolt;
import storm.rabbitmq.RabbitMQSpout;
import storm.rabbitmq.TupleToMessage;
import storm.rabbitmq.TupleToMessageNonDynamic;
import storm.rabbitmq.config.ConnectionConfig;
import storm.rabbitmq.config.ConsumerConfig;
import storm.rabbitmq.config.ConsumerConfigBuilder;
import storm.rabbitmq.config.ProducerConfig;

import storm.rabbitmq.config.ProducerConfigBuilder;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


/**
 * 
 * 创建人：Administrator <br>
 * 创建时间：2015-9-18 <br>
 * 功能描述：rabbitmq发布、消费消息测试 <br>
 * 版本： <br>
 * 版权拥有：深圳中青宝互动网络股份有限公司 <br>
 * ====================================== <br>
 *               修改记录 <br>
 * ====================================== <br>
 *  序号    姓名      日期      版本           简单描述 <br>
 *
 */
public class RabbitmqTest {

	public static class ExclamationBolt extends BaseBasicBolt {

	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	      declarer.declare(new Fields("result"));
	    }

	    public void execute(Tuple tuple, BasicOutputCollector collector) {
	      String arg = tuple.getString(0);
	      System.out.println(arg+"---"+tuple.getStringByField("msg"));
	      collector.emit(new Values(arg + "!"));
	    }

	  }
	
	/** 
	 *  
	 * @param args void
	 * @author:Administrator 
	 * @throws InterruptedException 
	 * @throws InvalidTopologyException 
	 * @throws AlreadyAliveException 
	 * @date:2015-9-17 
	 */
	public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {
		MessageScheme myMessageScheme = new MyMessageScheme();
		IRichSpout rabbitmqSpout = new RabbitMQSpout(myMessageScheme);
		ConnectionConfig connectionConfig = new ConnectionConfig("192.168.120.57", 5672, "ad", "ad_pass", ConnectionFactory.DEFAULT_VHOST, 10); // host, port, username, password, virtualHost, heartBeat 
		ConsumerConfig spoutConfig = new ConsumerConfigBuilder().connection(connectionConfig)
		                                                        .queue("app_queue")
		                                                        .prefetch(10)
		                                                        .requeueOnFail()
		                                                        .build();
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("my-spout", rabbitmqSpout)
		       .addConfigurations(spoutConfig.asMap())
		       .setMaxSpoutPending(10);
		builder.setBolt("exclaim", new ExclamationBolt(), 3).shuffleGrouping("my-spout");
		
		TupleToMessage scheme = new TupleToMessageNonDynamic() {
			  @Override
			protected
			  byte[] extractBody(Tuple input) { return input.getString(0).getBytes(); }
			};
		
		ProducerConfig sinkConfig = new ProducerConfigBuilder()
	    .connection(connectionConfig)
	    .contentEncoding("UTF-8")
	    .contentType("application/json")
	    .exchange("pay_exchange")
	    .routingKey("pay_key")
	    .build();

		builder.setBolt("rabbitmq-sink", new RabbitMQBolt(scheme))
		       .addConfigurations(sinkConfig.asMap())
		       .shuffleGrouping("exclaim");
		
		Config conf = new Config();
		conf.setDebug(true);
		if (args == null || args.length == 0) {
			LocalCluster cluster = new LocalCluster();			
			cluster.submitTopology("topology1", conf, builder.createTopology());
			//Thread.sleep(30000);
			//cluster.shutdown();
		}
		else {
			conf.setNumWorkers(2);
		    StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}
		
	}

}
