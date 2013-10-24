package storm.trident.redis;

import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.embedded.RedisServer;
import storm.trident.state.StateFactory;
import storm.trident.state.map.MapState;

import com.google.common.collect.Lists;
import com.google.common.io.Resources;

public class RedisTest {
	private static final long SEED = 8682522807148012L;
	private static final Random RANDOM = new Random(SEED);
	
	private static final String HOSTNAME = "localhost";
	private static final int PORT = 6379;
	
	private RedisServer redisServer;

	
	@Before
	public void setup() throws Exception {
        redisServer = new RedisServer(PORT);
        redisServer.start();
        cleanup();
	}
	
	@Test
	public void testCache() {
		try {
			StateFactory redis = RedisState.nonTransactional(new InetSocketAddress(HOSTNAME, PORT));
			MapState state = (MapState) redis.makeState(new HashMap(), null, 0, 0);
	
			// insert some key-value pairs
			String[] keys = new String[]{"foo", "bar", "baz"};
		    List<Integer> vals = Lists.newArrayList();
		    List<List<Object>> keyList = Lists.newArrayList();
		    for (String key : keys) {
		        List<Object> l = Lists.newArrayList();
		        l.add(key);
		        keyList.add(l);
		        vals.add(RANDOM.nextInt());
		    }
		    state.multiPut(keyList, vals);
		    
			// Verify the retrieval of the kv pairs.
			List<Integer> actualVals = state.multiGet(keyList);
			assertEquals(vals.size(), actualVals.size());
			for (int i = 0; i < vals.size(); i++) {
				assertEquals(vals.get(i), actualVals.get(i));
			}
		
		} catch (JedisConnectionException e) {
			throw new RuntimeException(String.format("Unfortunately, Jedis is unable to connect to redis server [%s:%i]", HOSTNAME, PORT), e);
		}
	}
	
	@After
	public void tearDown() {
		cleanup();
        redisServer.stop();
	}
	
	private void cleanup() {
		try {
			Jedis jedis = new Jedis(HOSTNAME, PORT);
			jedis.flushAll();
			jedis.disconnect();
			
		} catch (JedisConnectionException e) {
			throw new RuntimeException(String.format("Unfortunately, Jedis is unable to connect to redis server [%s:%i]", HOSTNAME, PORT), e);
		}
	}
}
