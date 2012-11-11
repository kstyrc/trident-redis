package storm.trident.redis;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
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
import backtype.storm.tuple.Values;

public class RedisState<T> implements IBackingMap<T> {
	private static final Map<StateType, Serializer> DEFAULT_SERIALIZERS = new HashMap<StateType, Serializer>() {
		{
			put(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
			put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
			put(StateType.OPAQUE, new JSONOpaqueSerializer());
		}
	};

	public static class Options<T> implements Serializable {
		public int localCacheSize = 1000;
		public String globalKey = "$REDIS-MAP-STATE-GLOBAL";
		public Serializer<T> serializer = null;
		public int connectionTimeout = Protocol.DEFAULT_TIMEOUT;
		public String password = null;
		public int database = Protocol.DEFAULT_DATABASE;
	}

	public static StateFactory opaque(InetSocketAddress server) {
		return opaque(server, new Options());
	}

	public static StateFactory opaque(InetSocketAddress server, Options<OpaqueValue> opts) {
		return new Factory(server, StateType.OPAQUE, opts);
	}

	public static StateFactory transactional(InetSocketAddress server) {
		return transactional(server, new Options());
	}

	public static StateFactory transactional(InetSocketAddress server, Options<TransactionalValue> opts) {
		return new Factory(server, StateType.TRANSACTIONAL, opts);
	}

	public static StateFactory nonTransactional(InetSocketAddress server) {
		return nonTransactional(server, new Options());
	}

	public static StateFactory nonTransactional(InetSocketAddress server, Options<Object> opts) {
		return new Factory(server, StateType.NON_TRANSACTIONAL, opts);
	}

	protected static class Factory implements StateFactory {
		StateType _type;
		InetSocketAddress _server;
		Serializer _ser;
		Options _opts;

		public Factory(InetSocketAddress server, StateType type, Options options) {
			_type = type;
			_server = server;
			_opts = options;
			if (options.serializer == null) {
				_ser = DEFAULT_SERIALIZERS.get(type);
				if (_ser == null) {
					throw new RuntimeException("Couldn't find serializer for state type: " + type);
				}
			} else {
				_ser = options.serializer;
			}
		}

		@Override
		public State makeState(Map conf, int partitionIndex, int numPartitions) {
			RedisState s;
			JedisPool pool = new JedisPool(new JedisPoolConfig(), 
					_server.getHostName(), _server.getPort(), _opts.connectionTimeout, _opts.password, _opts.database);
			s = new RedisState(pool, _opts, _ser);
			
			CachedMap c = new CachedMap(s, _opts.localCacheSize);
			MapState ms;
			if (_type == StateType.NON_TRANSACTIONAL) {
				ms = NonTransactionalMap.build(c);
			
			} else if (_type == StateType.OPAQUE) {
				ms = OpaqueMap.build(c);
			
			} else if (_type == StateType.TRANSACTIONAL) {
				ms = TransactionalMap.build(c);
			
			} else {
				throw new RuntimeException("Unknown state type: " + _type);
			}
			return new SnapshottableMap(ms, new Values(_opts.globalKey));
		}
	}
	
	private final JedisPool _pool;
	private Options _opts;
	private Serializer _ser;
	
	public RedisState(JedisPool pool, Options opts, Serializer<T> ser) {
		_pool = pool;
		_opts = opts;
		_ser  = ser;
	}
	
	private String makeKey(List<Object> key) {
		return null;
	}

	@Override
	public List<T> multiGet(List<List<Object>> keys) {
		String[] stringKeys = new String[keys.size()];
		int index = 0;
		for (List<Object> key : keys)
			stringKeys[index++] = makeKey(key);
		
		List<T> result = new ArrayList<T>(keys.size());
		Jedis jedis = _pool.getResource();
		try {
			List<String> values = jedis.mget(stringKeys);
			for (String value : values) {
				if (value != null) {
					result.add((T) _ser.deserialize(value.getBytes()));
				} else {
					result.add(null);
				}
			}
		} finally {
			_pool.returnResource(jedis);
		}
		
		return result;
	}

	@Override
	public void multiPut(List<List<Object>> keys, List<T> vals) {
		String[] keyValues = new String[keys.size() * 2];
		for (int i = 0; i < keys.size(); i++) {
			keyValues[i * 2] = makeKey(keys.get(i));
			keyValues[i * 2 + 1] = new String(_ser.serialize(vals.get(i)));
		}
		
		Jedis jedis = _pool.getResource();
		try {
			jedis.mset(keyValues);
		} finally {
			_pool.returnResource(jedis);
		}
	}
}
