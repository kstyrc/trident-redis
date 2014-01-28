package storm.trident.redis;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Protocol;
import storm.trident.state.JSONNonTransactionalSerializer;
import storm.trident.state.JSONOpaqueSerializer;
import storm.trident.state.JSONTransactionalSerializer;
import storm.trident.state.OpaqueValue;
import storm.trident.state.Serializer;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.SnapshottableMap;
import storm.trident.state.map.TransactionalMap;
import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class RedisState<T> implements IBackingMap<T> {
	private static final Logger logger = LoggerFactory.getLogger(RedisState.class);
	
	private static final EnumMap<StateType, Serializer> DEFAULT_SERIALIZERS = Maps.newEnumMap(ImmutableMap.of(
			StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer(),
			StateType.TRANSACTIONAL, new JSONTransactionalSerializer(),
			StateType.OPAQUE, new JSONOpaqueSerializer()
	));
	
	public static class DefaultKeyFactory implements KeyFactory {
		public String build(List<Object> key) {
			if (key.size() != 1)
				throw new RuntimeException("Default KeyFactory does not support compound keys");
			return (String) key.get(0);
		}
	};

	public static class Options<T> implements Serializable {
		public int localCacheSize = 1000;
		public String globalKey = "$REDIS-MAP-STATE-GLOBAL";
		public Serializer<T> serializer = null;
		public KeyFactory keyFactory = null;
		public int connectionTimeout = Protocol.DEFAULT_TIMEOUT;
		public String password = null;
		public int database = Protocol.DEFAULT_DATABASE;
		public String hkey = null;
	}
	
	public static interface KeyFactory extends Serializable {
		String build(List<Object> key);
	}

	public static StateFactory opaque(InetSocketAddress server) {
		return opaque(server, new Options());
	}
	
	public static StateFactory opaque(InetSocketAddress server, String hkey) {
		Options opts = new Options();
		opts.hkey = hkey;
		return opaque(server, opts);
	}
	
	public static StateFactory opaque(InetSocketAddress server, Options<OpaqueValue> opts) {
		return opaque(server, opts, new DefaultKeyFactory());
	}
	
	public static StateFactory opaque(InetSocketAddress server, Options<OpaqueValue> opts, KeyFactory factory) {
		return new Factory(server, StateType.OPAQUE, opts, factory);
	}

	public static StateFactory transactional(InetSocketAddress server) {
		return transactional(server, new Options());
	}
	
	public static StateFactory transactional(InetSocketAddress server, String hkey) {
		Options opts = new Options();
		opts.hkey = hkey;
		return transactional(server, opts);
	}
	
	public static StateFactory transactional(InetSocketAddress server, Options<TransactionalValue> opts) {
		return transactional(server, opts, new DefaultKeyFactory());
	}
	
	public static StateFactory transactional(InetSocketAddress server, Options<TransactionalValue> opts, KeyFactory factory) {
		return new Factory(server, StateType.TRANSACTIONAL, opts, factory);
	}

	public static StateFactory nonTransactional(InetSocketAddress server) {
		return nonTransactional(server, new Options());
	}
	
	public static StateFactory nonTransactional(InetSocketAddress server, String hkey) {
		Options opts = new Options();
		opts.hkey = hkey;
		return nonTransactional(server, opts);
	}
	
	public static StateFactory nonTransactional(InetSocketAddress server, Options<Object> opts) {
		return nonTransactional(server, opts, new DefaultKeyFactory());
	}
	
	public static StateFactory nonTransactional(InetSocketAddress server, Options<Object> opts, KeyFactory factory) {
		return new Factory(server, StateType.NON_TRANSACTIONAL, opts, factory);
	}

	protected static class Factory implements StateFactory {
		StateType type;
		InetSocketAddress server;
		Serializer serializer;
		KeyFactory factory;
		Options options;

		public Factory(InetSocketAddress server, StateType type, Options options, KeyFactory factory) {
			this.type = type;
			this.server = server;
			this.options = options;
			this.factory = factory;
			
			if (options.serializer == null) {
				serializer = DEFAULT_SERIALIZERS.get(type);
				if (serializer == null) {
					throw new RuntimeException("Couldn't find serializer for state type: " + type);
				}
			} else {
				this.serializer = options.serializer;
			}
		}

		public State makeState(@SuppressWarnings("rawtypes") Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			JedisPool pool = new JedisPool(new JedisPoolConfig(), 
					server.getHostName(), server.getPort(), options.connectionTimeout, options.password, options.database);
			RedisState state = new RedisState(pool, options, serializer, factory);
			CachedMap c = new CachedMap(state, options.localCacheSize);
			
			MapState ms;
			if (type == StateType.NON_TRANSACTIONAL) {
				ms = NonTransactionalMap.build(c);
			
			} else if (type == StateType.OPAQUE) {
				ms = OpaqueMap.build(c);
			
			} else if (type == StateType.TRANSACTIONAL) {
				ms = TransactionalMap.build(c);
			
			} else {
				throw new RuntimeException("Unknown state type: " + type);
			}
			
			return new SnapshottableMap(ms, new Values(options.globalKey));
		}
	}
	
	private final JedisPool pool;
	private Options options;
	private Serializer serializer;
	private KeyFactory keyFactory;
	
	public RedisState(JedisPool pool, Options options, Serializer<T> serializer, KeyFactory keyFactory) {
		this.pool = pool;
		this.options = options;
		this.serializer = serializer;
		this.keyFactory = keyFactory;
	}

	public List<T> multiGet(List<List<Object>> keys) {
		if (keys.size() > 0) {
			List<T> result = new ArrayList<T>(keys.size());
			Jedis jedis = pool.getResource();
			
			try {
				/*
				 * mget
				 */
				if (Strings.isNullOrEmpty(this.options.hkey)) {
					String[] stringKeys = new String[keys.size()];
					int index = 0;
					for (List<Object> key : keys)
						stringKeys[index++] = keyFactory.build(key);
				
					List<String> values = jedis.mget(stringKeys);
					for (String value : values) {
						if (value != null) {
							result.add((T) serializer.deserialize(value.getBytes()));
						} else {
							result.add(null);
						}
					}
				/*
				 * hset with a first-level key
				 */
				} else {
					Map<byte[], byte[]> keyvalueMap = jedis.hgetAll(this.options.hkey.getBytes());
					for (List<Object> key : keys) {
						String strKey = keyFactory.build(key);
						byte[] valBytes = keyvalueMap.get(strKey.getBytes());
						if (valBytes != null) {
							result.add((T) serializer.deserialize(valBytes));
							logger.trace(String.format("get %s : %s", strKey, String.valueOf(valBytes)));
						} else {
							result.add(null);
							logger.trace(String.format("get %s : null", strKey));
						}
					}
					logger.info(String.format("restore from redis: %s", this.options.hkey));
				}
			} finally {
				pool.returnResource(jedis);
			}
			
			return result;
		
		} else {
			return new ArrayList<T>();
		}
	}

	public void multiPut(List<List<Object>> keys, List<T> vals) {
		if (keys.size() > 0) {
			Jedis jedis = pool.getResource();

			/*
			 * mset 
			 */
			if (Strings.isNullOrEmpty(this.options.hkey)) {
				String[] keyValues = new String[keys.size() * 2];
				for (int i = 0; i < keys.size(); i++) {
					keyValues[i * 2] = keyFactory.build(keys.get(i));
					keyValues[i * 2 + 1] = new String(serializer.serialize(vals.get(i)));
				}
				
				try {
					jedis.mset(keyValues);
				} finally {
					pool.returnResource(jedis);
				}
			/*
			 * hset with a first-level key
			 */
			} else {
				Pipeline pl = jedis.pipelined();  
				pl.multi();
				
				for (int i = 0; i < keys.size(); i++) {
					String val = new String(serializer.serialize(vals.get(i)));
					pl.hset(this.options.hkey, 
							keyFactory.build(keys.get(i)), 
							val);
					logger.trace(String.format("put %s : %s", keyFactory.build(keys.get(i)), val));
				}
				
				pl.exec();
				pl.sync();
				logger.info(String.format("flush to redis: %s", this.options.hkey));
			}
		}
	}
}
