package storm.rabbitmq.test;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import storm.rabbitmq.Message;
import storm.rabbitmq.MessageScheme;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 
 * 创建人：Administrator <br>
 * 创建时间：2015-9-18 <br>
 * 功能描述：自定义消息格式 <br>
 * 版本： <br>
 * 版权拥有：深圳中青宝互动网络股份有限公司 <br>
 * ====================================== <br>
 *               修改记录 <br>
 * ====================================== <br>
 *  序号    姓名      日期      版本           简单描述 <br>
 *
 */
public class MyMessageScheme implements MessageScheme
{

	@Override
	public List<Object> deserialize(byte[] arg0) {
		try {
            String msg = new String(arg0, "UTF-8"); 
            return new Values(msg);
        } catch (UnsupportedEncodingException e) {  
         
        }
        return null;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("msg");
	}

	@Override
	public void open(Map config, TopologyContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		System.out.println("closing......");
	}

	@Override
	public List<Object> deserialize(Message message) {
		if(message == null || message.getBody() == null)
		{
			return null;
		}
		try {
            String msg = new String(message.getBody(), "UTF-8"); 
            return new Values(msg);
        } catch (UnsupportedEncodingException e) {  
         
        }
        return null;
	}
	
}
