package storm.trident.redis;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.embedded.RedisServer;
import storm.trident.state.StateFactory;
import storm.trident.state.map.MapState;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

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
      RedisState.Options<Object> opts = new RedisState.Options<Object>();
      opts.localCacheSize = 0;
      StateFactory redis = RedisState.nonTransactional(new InetSocketAddress(HOSTNAME, PORT), opts);
      MapState state = (MapState) redis.makeState(new HashMap(), null, 0, 0);

      List<List<Object>> keys = Arrays.asList(
            Arrays.<Object>asList("foo"),
            Arrays.<Object>asList("bar"),
            Arrays.<Object>asList("bazz")
      );
      List<Integer> values = Arrays.asList(
            RANDOM.nextInt(), RANDOM.nextInt(), RANDOM.nextInt()
      );
      insertKeyValues(state, keys, values);
      verifyKeyValues(state, keys, values);
   }

   @Test
   public void testCacheWithCustomKeyFactory() throws Exception {
      RedisState.Options<Object> opts = new RedisState.Options<Object>();
      opts.localCacheSize = 1;
      StateFactory redis = RedisState.nonTransactional(
            new InetSocketAddress(HOSTNAME, PORT),
            opts,
            new RedisState.KeyFactory() {
               @Override
               public String build(List<Object> key) {
                  if (key.size() >= 2) {
                     return key.get(0).toString() + key.get(1).toString();
                  } else {
                     return key.get(0).toString();
                  }
               }
            });
      MapState state = (MapState) redis.makeState(new HashMap(), null, 0, 0);

      List<List<Object>> keys = Arrays.asList(
            Arrays.<Object>asList("foo", "1", "2"),
            Arrays.<Object>asList("foo", "2", "3"),
            Arrays.<Object>asList("bazz", "1", "2")
      );
      List<Integer> values = Arrays.asList(
            RANDOM.nextInt(), RANDOM.nextInt(), RANDOM.nextInt()
      );
      insertKeyValues(state, keys, values);
      verifyKeyValues(state, keys, values);
   }

   private void insertKeyValues(MapState state, List<List<Object>> keys, List<Integer> values) {
      state.multiPut(keys, values);
   }

   private void verifyKeyValues(MapState state, List<List<Object>> keys, List<Integer> values) {
      List<?> actualValues = state.multiGet(keys);
      assertEquals(values.size(), actualValues.size());
      for (int i = 0; i < values.size(); i++) {
         assertEquals(values.get(i).toString(), actualValues.get(i).toString());
      }
   }

   @After
   public void tearDown() {
      cleanup();
       try {
           redisServer.stop();
       } catch (InterruptedException e) {
           e.printStackTrace();
       }
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
