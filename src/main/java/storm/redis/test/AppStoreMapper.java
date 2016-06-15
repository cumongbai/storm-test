package storm.redis.test;

import backtype.storm.tuple.ITuple;
import storm.redis.common.mapper.RedisDataTypeDescription;
import storm.redis.common.mapper.RedisStoreMapper;

public class AppStoreMapper implements RedisStoreMapper {
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
