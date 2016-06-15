package storm.redis.test;

import java.util.ArrayList;
import java.util.List;

import storm.redis.common.mapper.RedisDataTypeDescription;
import storm.redis.common.mapper.RedisLookupMapper;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.ITuple;
import backtype.storm.tuple.Values;

public class AppLookupMapper implements RedisLookupMapper {
    @Override
    public List<Values> toTuple(ITuple input, Object value) {
        List<Values> values = new ArrayList<Values>();
        values.add(new Values(getKeyFromTuple(input), value));
        return values;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("app_id-ad_code", "count"));
    }

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.HASH, "app");
    }

    @Override
    public String getKeyFromTuple(ITuple tuple) {
        return tuple.getStringByField("app_id")+"_"+tuple.getStringByField("ad_code");
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
        return tuple.getInteger(1).toString();
    }
}
