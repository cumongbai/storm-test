package storm.rabbitmq.test;

import storm.rabbitmq.RabbitMQConsumer;
import storm.rabbitmq.RabbitMQSpout;
import storm.rabbitmq.config.ConnectionConfig;
import storm.rabbitmq.config.ConsumerConfig;
import storm.rabbitmq.config.ConsumerConfigBuilder;

import backtype.storm.topology.IRichSpout;

import com.rabbitmq.client.ConnectionFactory;

public class RabbitmqTest2 {

	/** 
	 *  
	 * @param args void
	 * @author:Administrator 
	 * @date:2015-9-17 
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		ConnectionConfig connectionConfig = new ConnectionConfig("localhost", 5672, "guest", "guest", ConnectionFactory.DEFAULT_VHOST, 10); // host, port, username, password, virtualHost, heartBeat 
		ConsumerConfig spoutConfig = new ConsumerConfigBuilder().connection(connectionConfig)
		                                                        .queue("pay_queue")
		                                                        .prefetch(10)
		                                                        .requeueOnFail()
		                                                        .build();
		RabbitMQConsumer reConsumer = new RabbitMQConsumer(connectionConfig, 10, "pay_queue", false, null, null);

	}

}
