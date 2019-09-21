package org.wuzl.util.redis;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.pool2.impl.BaseObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.util.SafeEncoder;

/**
 * redis工具类</br>
 * <ul>
 * <li><b>该类需要调用者在调用项目中提供配置文件(参照redis-example.properties)：</b></br>
 * 配置文件重命名为redis. properties， 一般只需传入链接信息，其他都有默认值</li>
 * <li><b>配置文件需要的配置项：</b></br>
 * </li>
 * </ul>
 * 
 * @author gaoyang
 */
public class JedisUtil {

	private static final Logger error_logger = LoggerFactory.getLogger("PROJECT_ERROR");

	private static final Logger info_log = LoggerFactory.getLogger("PROJECT_INFO");
	/**
	 * 默认的redis配置文件名称
	 */
	private static String FILE_NAME = "redis.properties";

	private static JedisPool jedisPool = null;
	private ShardedJedisPool shardedJedisPool = null;

	// 默认链接池参数
	/**
	 * 连接池中最大连接数
	 */
	private static Integer MAXTOTAL = 300;

	/**
	 * 连接池中最大空闲的连接数
	 */
	private static Integer MAXIDLE = 200;
	/**
	 * 连接池中最少空闲的连接数
	 */
	private static Integer MINIDLE = 200;
	/**
	 * 当连接池资源耗尽时，调用者最大阻塞的时间，超时将跑出异常。单位，毫秒数;默认为-1.表示永不超时
	 */
	private static Integer MAXWAITMILLIS = 10000;
	/**
	 * 连接空闲的最小时间，达到此值后空闲连接将可能会被移除。负值(-1)表示不移除.
	 */
	@SuppressWarnings("unused")
	private static Integer MINEVICTABLEIDLETIMEMILLIS = 1800000;
	/**
	 * 对于“空闲链接”检测线程而言，每次检测的链接资源的个数
	 */
	@SuppressWarnings("unused")
	private static Integer NUMTESTSPEREVICTIONRUN = BaseObjectPoolConfig.DEFAULT_NUM_TESTS_PER_EVICTION_RUN;
	/**
	 * 空闲链接”检测线程，检测的周期，毫秒数。如果为负值，表示不运行“检测线程”。默认为-1
	 */
	@SuppressWarnings("unused")
	private static Long TIMEBETWEENEVICTIONRUNSMILLIS = BaseObjectPoolConfig.DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS;
	@SuppressWarnings("unused")
	private static Boolean TESTONBORROW = BaseObjectPoolConfig.DEFAULT_TEST_ON_BORROW;
	@SuppressWarnings("unused")
	private static Boolean TESTONRETURN = BaseObjectPoolConfig.DEFAULT_TEST_ON_RETURN;
	/**
	 * 静态属性的方式加载实例
	 */
	private static JedisUtil redis = new JedisUtil();
	/**
	 * 操作Key的方法
	 */
	public static JedisUtil.Keys KEYS = redis.new Keys();
	/**
	 * 对存储结构为String类型的操作
	 */
	public static JedisUtil.Strings STRINGS = redis.new Strings();
	/**
	 * 对存储结构为List类型的操作
	 */
	public static JedisUtil.Lists LISTS = redis.new Lists();
	/**
	 * 对存储结构为Set类型的操作
	 */
	public static JedisUtil.Sets SETS = redis.new Sets();
	/**
	 * 对存储结构为HashMap类型的操作
	 */
	public static JedisUtil.Hash HASH = redis.new Hash();
	/**
	 * 对存储结构为Set(排序的)类型的操作
	 */
	public static JedisUtil.SortSet SORTSET = redis.new SortSet();

	private static AtomicBoolean isInited = new AtomicBoolean(false);

	private JedisUtil() {

	}

	/**
	 * 构建redis连接池
	 * 
	 * @param ip
	 * @param port
	 * @return JedisPool
	 * @throws Exception
	 */
	public static void init() throws Exception {
		if (!isInited.compareAndSet(false, true)) {
			info_log.info("JedisUtil has bean inited.");
			return;
		}
		info_log.info("JedisUtil is being inited.........");
		// 读取配置文件，并封装为properties
		Properties prop = loadRedisProperties();
		// 校验配置
		checkParameter(prop);

		if (jedisPool == null) {
			// 构建连接池参数
			JedisPoolConfig config = createPoolConfig(prop);

			String host = prop.getProperty("redis.pool.host");
			Integer port = Integer.valueOf(prop.getProperty("redis.pool.port"));
			Integer timeOut = Protocol.DEFAULT_TIMEOUT;
			String timeOutStr = prop.getProperty("redis.pool.timeout");
			if (timeOutStr != null && timeOutStr.length() > 0) {
				timeOut = Integer.valueOf(timeOutStr);
			}
			String password = prop.getProperty("redis.pool.password");
			Integer dataBase = Protocol.DEFAULT_DATABASE;
			String dataBaseStr = prop.getProperty("redis.pool.database");
			if (dataBaseStr != null && dataBaseStr.length() > 0) {
				dataBase = Integer.valueOf(dataBaseStr);
			}
			jedisPool = new JedisPool(config, host, port, timeOut, password, dataBase);

			info_log.info("JedisUtil has been inited.........");
		}
	}

	private static Properties loadRedisProperties() {
		Properties prop = null;
		// 读取配置文件
		InputStream is = JedisUtil.class.getClassLoader().getResourceAsStream(FILE_NAME);
		if (is == null) {
			error_logger.error("can not find redis properties,please check classpath has file " + FILE_NAME);
			return prop;
		}
		// 配置文件信息加载到properties中
		prop = new Properties();
		try {
			prop.load(is);
		} catch (IOException e) {
			error_logger.error("load redis properties error", e);
		} finally {
			try {
				is.close();
			} catch (IOException e) {
				error_logger.error("close redis properties resource error", e);
			}
		}
		return prop;
	}

	private static JedisPoolConfig createPoolConfig(Properties prop) {
		JedisPoolConfig config = new JedisPoolConfig();

		String maxTotalStr = prop.getProperty("redis.pool.maxTotal");
		if (maxTotalStr == null || maxTotalStr.length() == 0) {
			config.setMaxTotal(MAXTOTAL);
		} else {
			config.setMaxTotal(Integer.valueOf(maxTotalStr));
		}

		String maxIdleStr = prop.getProperty("redis.pool.maxIdle");
		if (maxIdleStr == null || maxIdleStr.length() == 0) {
			config.setMaxIdle(MAXIDLE);
		} else {
			config.setMaxIdle(Integer.valueOf(maxIdleStr));
		}

		String minIdleStr = prop.getProperty("redis.pool.minIdle");
		if (minIdleStr == null || minIdleStr.length() == 0) {
			config.setMinIdle(MINIDLE);
		} else {
			config.setMinIdle(Integer.valueOf(minIdleStr));
		}

		String maxWaitMillisStr = prop.getProperty("redis.pool.maxWaitMillis");
		if (maxWaitMillisStr == null || maxWaitMillisStr.length() == 0) {
			config.setMaxWaitMillis(MAXWAITMILLIS);
		} else {
			config.setMaxWaitMillis(Integer.valueOf(maxWaitMillisStr));
		}

		String minEvictableIdleTimeMillisStr = prop.getProperty("redis.pool.minEvictableIdleTimeMillis");
		if (minEvictableIdleTimeMillisStr != null && minEvictableIdleTimeMillisStr.length() > 0) {
			config.setMinEvictableIdleTimeMillis(Long.valueOf(minEvictableIdleTimeMillisStr));
		}

		String numTestsPerEvictionRunStr = prop.getProperty("redis.pool.numTestsPerEvictionRun");
		if (numTestsPerEvictionRunStr != null && numTestsPerEvictionRunStr.length() > 0) {
			config.setNumTestsPerEvictionRun(Integer.valueOf(numTestsPerEvictionRunStr));
		}

		String timeBetweenEvictionRunsMillisStr = prop.getProperty("redis.pool.timeBetweenEvictionRunsMillis");
		if (timeBetweenEvictionRunsMillisStr != null && timeBetweenEvictionRunsMillisStr.length() > 0) {
			config.setTimeBetweenEvictionRunsMillis(Long.valueOf(timeBetweenEvictionRunsMillisStr));
		}
		String testOnBorrowStr = prop.getProperty("redis.pool.testOnBorrow");
		if (testOnBorrowStr != null && testOnBorrowStr.length() > 0) {
			config.setTestOnBorrow(Boolean.valueOf(testOnBorrowStr));
		}

		String testOnReturnStr = prop.getProperty("redis.pool.testOnReturn");
		if (testOnReturnStr != null && testOnReturnStr.length() > 0) {
			config.setTestOnReturn(Boolean.valueOf(testOnReturnStr));
		}
		return config;
	}

	private static void checkParameter(Properties prop) {
		if (prop == null) {
			error_logger.error("not find redis properties");
			throw new IllegalArgumentException("[redis.properties] is not found!");
		}
		// 校验参数
		String host = prop.getProperty("redis.pool.host");
		if (host == null || host.length() == 0) {
			error_logger.error("miss redis host Parameter host");
			throw new IllegalArgumentException("[redis.properties.host] is not found!");
		}
		String port = prop.getProperty("redis.pool.port");
		if (port == null || port.length() == 0) {
			error_logger.error("miss redis host Parameter port");
			throw new IllegalArgumentException("[redis.properties.port] is not found!");
		}
		String password = prop.getProperty("redis.pool.password");
		if (password == null || password.length() == 0) {
			error_logger.error("miss redis host Parameter password");
			throw new IllegalArgumentException("[redis.properties.password] is not found!");
		}
	}

	public JedisPool getPool() {
		return jedisPool;
	}

	public ShardedJedisPool getShardedJedisPool() {
		return shardedJedisPool;
	}

	/**
	 * 从jedis连接池中获取获取jedis对象 wuzl加入同步代码
	 * 
	 * @return
	 */
	public Jedis getJedis() {
		return jedisPool.getResource();
	}

	/**
	 * @param 选择指定DB
	 * @return
	 */
	public Jedis getJedis(int DBindex) {
		Jedis jedis = jedisPool.getResource();
		jedis.select(DBindex);
		return jedis;
	}

	/**
	 * 获取JedisUtil实例
	 * 
	 * @return
	 */
	public static JedisUtil getInstance() {
		return redis;
	}

	/**
	 * 回收jedis
	 * 
	 * @param jedis
	 */
	public void returnJedis(Jedis jedis) {
		// jedisPool.returnResource(jedis);
		jedis.close();
	}

	/**
	 * 设置过期时间
	 *
	 * @param key
	 * @param seconds
	 */
	public void expire(String key, int seconds) {
		if (seconds <= 0) {
			return;
		}
		Jedis jedis = getJedis();
		jedis.expire(key, seconds);
		returnJedis(jedis);
	}

	public void expire(String key, int seconds, int DBindex) {
		if (seconds <= 0) {
			return;
		}
		Jedis jedis = getJedis(DBindex);
		jedis.expire(key, seconds);
		returnJedis(jedis);
	}

	/**
	 * redis keys的操作
	 * 
	 * @author gaoyang
	 */
	public class Keys {

		/**
		 * 清空所有key
		 */
		public String flushAll() {
			Jedis jedis = getJedis();
			String stata = jedis.flushAll();
			returnJedis(jedis);
			return stata;
		}

		/**
		 * 更改key
		 *
		 * @param String
		 *            oldkey
		 * @param String
		 *            newkey
		 * @return 状态码
		 */
		public String rename(String oldkey, String newkey) {
			return rename(SafeEncoder.encode(oldkey), SafeEncoder.encode(newkey));
		}

		/**
		 * 更改key,仅当新key不存在时才执行
		 *
		 * @param String
		 *            oldkey
		 * @param String
		 *            newkey
		 * @return 状态码
		 */
		public long renamenx(String oldkey, String newkey) {
			Jedis jedis = getJedis();
			long status = jedis.renamenx(oldkey, newkey);
			returnJedis(jedis);
			return status;
		}

		/**
		 * 更改key
		 *
		 * @param String
		 *            oldkey
		 * @param String
		 *            newkey
		 * @return 状态码
		 */
		public String rename(byte[] oldkey, byte[] newkey) {
			Jedis jedis = getJedis();
			String status = jedis.rename(oldkey, newkey);
			returnJedis(jedis);
			return status;
		}

		/**
		 * 设置key的过期时间，以秒为单位
		 *
		 * @param String
		 *            key
		 * @param 时间
		 *            ,已秒为单位
		 * @return 影响的记录数
		 */
		public long expired(String key, int seconds) {
			Jedis jedis = getJedis();
			long count = jedis.expire(key, seconds);
			returnJedis(jedis);
			return count;
		}

		public long expired(String key, int seconds, int DBindex) {
			Jedis jedis = getJedis(DBindex);
			long count = jedis.expire(key, seconds);
			returnJedis(jedis);
			return count;
		}

		/**
		 * 设置key的过期时间,它是距历元（即格林威治标准时间 1970 年 1 月 1 日的 00:00:00，格里高利历）的偏移量。
		 *
		 * @param String
		 *            key
		 * @param 时间
		 *            ,已秒为单位
		 * @return 影响的记录数
		 */
		public long expireAt(String key, long timestamp) {
			Jedis jedis = getJedis();
			long count = jedis.expireAt(key, timestamp);
			returnJedis(jedis);
			return count;
		}

		/**
		 * 查询key的过期时间
		 *
		 * @param String
		 *            key
		 * @return 以秒为单位的时间表示
		 */
		public long ttl(String key) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			long len = sjedis.ttl(key);
			returnJedis(sjedis);
			return len;
		}

		/**
		 * 取消对key过期时间的设置
		 *
		 * @param key
		 * @return 影响的记录数
		 */
		public long persist(String key) {
			Jedis jedis = getJedis();
			long count = jedis.persist(key);
			returnJedis(jedis);
			return count;
		}

		/**
		 * 删除keys对应的记录,可以是多个key
		 *
		 * @param String
		 *            ... keys
		 * @return 删除的记录数
		 */
		public long del(String... keys) {
			Jedis jedis = getJedis();
			long count = jedis.del(keys);
			returnJedis(jedis);
			return count;
		}

		public long del(int DBindex, String... keys) {
			Jedis jedis = getJedis(DBindex);
			long count = jedis.del(keys);
			returnJedis(jedis);
			return count;
		}

		/**
		 * 删除keys对应的记录,可以是多个key
		 *
		 * @param String
		 *            ... keys
		 * @return 删除的记录数
		 */
		public long del(byte[]... keys) {
			Jedis jedis = getJedis();
			long count = jedis.del(keys);
			returnJedis(jedis);
			return count;
		}

		/**
		 * 判断key是否存在
		 *
		 * @param String
		 *            key
		 * @return boolean
		 */
		public boolean exists(String key) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			boolean exis = sjedis.exists(key);
			returnJedis(sjedis);
			return exis;
		}

		public boolean exists(String key, int DBindex) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis(DBindex);
			boolean exis = sjedis.exists(key);
			returnJedis(sjedis);
			return exis;
		}

		/**
		 * 对List,Set,SortSet进行排序,如果集合数据较大应避免使用这个方法
		 *
		 * @param String
		 *            key
		 * @return List<String> 集合的全部记录
		 **/
		public List<String> sort(String key) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			List<String> list = sjedis.sort(key);
			returnJedis(sjedis);
			return list;
		}

		/**
		 * 对List,Set,SortSet进行排序或limit
		 *
		 * @param String
		 *            key
		 * @param SortingParams
		 *            parame 定义排序类型或limit的起止位置.
		 * @return List<String> 全部或部分记录
		 **/
		public List<String> sort(String key, SortingParams parame) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			List<String> list = sjedis.sort(key, parame);
			returnJedis(sjedis);
			return list;
		}

		/**
		 * 返回指定key存储的类型
		 *
		 * @param String
		 *            key
		 * @return String string|list|set|zset|hash
		 **/
		public String type(String key) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			String type = sjedis.type(key);
			returnJedis(sjedis);
			return type;
		}

		/**
		 * 查找所有匹配给定的模式的键
		 *
		 * @param String
		 *            key的表达式,*表示多个，？表示一个
		 */
		public Set<String> keys(String pattern) {
			Jedis jedis = getJedis();
			Set<String> set = jedis.keys(pattern);
			returnJedis(jedis);
			return set;
		}

		public ScanResult<String> scan(String cursor) {
			Jedis jedis = getJedis();
			ScanResult<String> result = jedis.scan(cursor);
			returnJedis(jedis);
			return result;
		}
	}

	/**
	 * Set类型操作类
	 * 
	 * @author gaoyang
	 */
	public class Sets {

		/**
		 * 向Set添加一条记录，如果member已存在返回0,否则返回1
		 *
		 * @param String
		 *            key
		 * @param String
		 *            member
		 * @return 操作码,0或1
		 */
		public long sadd(String key, String member) {
			Jedis jedis = getJedis();
			long s = jedis.sadd(key, member);
			returnJedis(jedis);
			return s;
		}

		public long sadd(String key, String member, int DBindex) {
			Jedis jedis = getJedis(DBindex);
			long s = jedis.sadd(key, member);
			returnJedis(jedis);
			return s;
		}

		public long sadd(byte[] key, byte[] member) {
			Jedis jedis = getJedis();
			long s = jedis.sadd(key, member);
			returnJedis(jedis);
			return s;
		}

		public long sadd(byte[] key, byte[] member, int DBindex) {
			Jedis jedis = getJedis(DBindex);
			long s = jedis.sadd(key, member);
			returnJedis(jedis);
			return s;
		}

		/**
		 * 获取给定key中元素个数
		 *
		 * @param String
		 *            key
		 * @return 元素个数
		 */
		public long scard(String key) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			long len = sjedis.scard(key);
			returnJedis(sjedis);
			return len;
		}

		public long scard(String key, int DBindex) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis(DBindex);
			long len = sjedis.scard(key);
			returnJedis(sjedis);
			return len;
		}

		/**
		 * 返回从第一组和所有的给定集合之间的差异的成员
		 *
		 * @param String
		 *            ... keys
		 * @return 差异的成员集合
		 */
		public Set<String> sdiff(String... keys) {
			Jedis jedis = getJedis();
			Set<String> set = jedis.sdiff(keys);
			returnJedis(jedis);
			return set;
		}

		public Set<String> sdiff(int DBindex, String... keys) {
			Jedis jedis = getJedis(DBindex);
			Set<String> set = jedis.sdiff(keys);
			returnJedis(jedis);
			return set;
		}

		/**
		 * 这个命令等于sdiff,但返回的不是结果集,而是将结果集存储在新的集合中，如果目标已存在，则覆盖。
		 *
		 * @param String
		 *            newkey 新结果集的key
		 * @param String
		 *            ... keys 比较的集合
		 * @return 新集合中的记录数
		 **/
		public long sdiffstore(int DBindex, String newkey, String... keys) {
			Jedis jedis = getJedis(DBindex);
			long s = jedis.sdiffstore(newkey, keys);
			returnJedis(jedis);
			return s;
		}

		public long sdiffstore(String newkey, String... keys) {
			Jedis jedis = getJedis();
			long s = jedis.sdiffstore(newkey, keys);
			returnJedis(jedis);
			return s;
		}

		/**
		 * 返回给定集合交集的成员,如果其中一个集合为不存在或为空，则返回空Set
		 *
		 * @param String
		 *            ... keys
		 * @return 交集成员的集合
		 **/
		public Set<String> sinter(String... keys) {
			Jedis jedis = getJedis();
			Set<String> set = jedis.sinter(keys);
			returnJedis(jedis);
			return set;
		}

		public Set<String> sinter(int DBindex, String... keys) {
			Jedis jedis = getJedis(DBindex);
			Set<String> set = jedis.sinter(keys);
			returnJedis(jedis);
			return set;
		}

		/**
		 * 这个命令等于sinter,但返回的不是结果集,而是将结果集存储在新的集合中，如果目标已存在，则覆盖。
		 *
		 * @param String
		 *            newkey 新结果集的key
		 * @param String
		 *            ... keys 比较的集合
		 * @return 新集合中的记录数
		 **/
		public long sinterstore(String newkey, String... keys) {
			Jedis jedis = getJedis();
			long s = jedis.sinterstore(newkey, keys);
			returnJedis(jedis);
			return s;
		}

		public long sinterstore(int DBindex, String newkey, String... keys) {
			Jedis jedis = getJedis(DBindex);
			long s = jedis.sinterstore(newkey, keys);
			returnJedis(jedis);
			return s;
		}

		/**
		 * 确定一个给定的值是否存在
		 *
		 * @param String
		 *            key
		 * @param String
		 *            member 要判断的值
		 * @return 存在返回1，不存在返回0
		 **/
		public boolean sismember(String key, String member) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			boolean s = sjedis.sismember(key, member);
			returnJedis(sjedis);
			return s;
		}

		public boolean sismember(int DBindex, String key, String member) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis(DBindex);
			boolean s = sjedis.sismember(key, member);
			returnJedis(sjedis);
			return s;
		}

		/**
		 * 返回集合中的所有成员
		 *
		 * @param String
		 *            key
		 * @return 成员集合
		 */
		public Set<String> smembers(String key) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			Set<String> set = sjedis.smembers(key);
			returnJedis(sjedis);
			return set;
		}

		public Set<String> smembers(String key, int DBindex) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis(DBindex);
			Set<String> set = sjedis.smembers(key);
			returnJedis(sjedis);
			return set;
		}

		public Set<byte[]> smembers(byte[] key) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			Set<byte[]> set = sjedis.smembers(key);
			returnJedis(sjedis);
			return set;
		}

		public Set<byte[]> smembers(byte[] key, int DBindex) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis(DBindex);
			Set<byte[]> set = sjedis.smembers(key);
			returnJedis(sjedis);
			return set;
		}

		/**
		 * 将成员从源集合移出放入目标集合 <br/>
		 * 如果源集合不存在或不包哈指定成员，不进行任何操作，返回0<br/>
		 * 否则该成员从源集合上删除，并添加到目标集合，如果目标集合中成员已存在，则只在源集合进行删除
		 *
		 * @param String
		 *            srckey 源集合
		 * @param String
		 *            dstkey 目标集合
		 * @param String
		 *            member 源集合中的成员
		 * @return 状态码，1成功，0失败
		 */
		public long smove(String srckey, String dstkey, String member) {
			Jedis jedis = getJedis();
			long s = jedis.smove(srckey, dstkey, member);
			returnJedis(jedis);
			return s;
		}

		public long smove(String srckey, String dstkey, String member, int DBindex) {
			Jedis jedis = getJedis(DBindex);
			long s = jedis.smove(srckey, dstkey, member);
			returnJedis(jedis);
			return s;
		}

		/**
		 * 从集合中删除成员
		 *
		 * @param String
		 *            key
		 * @return 被删除的成员
		 */
		public String spop(String key) {
			Jedis jedis = getJedis();
			String s = jedis.spop(key);
			returnJedis(jedis);
			return s;
		}

		public String spop(String key, int DBindex) {
			Jedis jedis = getJedis(DBindex);
			String s = jedis.spop(key);
			returnJedis(jedis);
			return s;
		}

		/**
		 * 从集合中删除指定成员
		 *
		 * @param String
		 *            key
		 * @param String
		 *            member 要删除的成员
		 * @return 状态码，成功返回1，成员不存在返回0
		 */
		public long srem(String key, String member) {
			Jedis jedis = getJedis();
			long s = jedis.srem(key, member);
			returnJedis(jedis);
			return s;
		}

		public long srem(String key, String member, int DBindex) {
			Jedis jedis = getJedis(DBindex);
			long s = jedis.srem(key, member);
			returnJedis(jedis);
			return s;
		}

		/**
		 * 合并多个集合并返回合并后的结果，合并后的结果集合并不保存<br/>
		 *
		 * @param String
		 *            ... keys
		 * @return 合并后的结果集合
		 * @see sunionstore
		 */
		public Set<String> sunion(String... keys) {
			Jedis jedis = getJedis();
			Set<String> set = jedis.sunion(keys);
			returnJedis(jedis);
			return set;
		}

		public Set<String> sunion(int DBindex, String... keys) {
			Jedis jedis = getJedis(DBindex);
			Set<String> set = jedis.sunion(keys);
			returnJedis(jedis);
			return set;
		}

		/**
		 * 合并多个集合并将合并后的结果集保存在指定的新集合中，如果新集合已经存在则覆盖
		 *
		 * @param String
		 *            newkey 新集合的key
		 * @param String
		 *            ... keys 要合并的集合
		 **/
		public long sunionstore(String newkey, String... keys) {
			Jedis jedis = getJedis();
			long s = jedis.sunionstore(newkey, keys);
			returnJedis(jedis);
			return s;
		}

		public long sunionstore(int DBindex, String newkey, String... keys) {
			Jedis jedis = getJedis(DBindex);
			long s = jedis.sunionstore(newkey, keys);
			returnJedis(jedis);
			return s;
		}
	}

	// *******************************************SortSet*******************************************//
	public class SortSet {

		/**
		 * 向集合中增加一条记录,如果这个值已存在，这个值对应的权重将被置为新的权重
		 *
		 * @param String
		 *            key
		 * @param double
		 *            score 权重
		 * @param String
		 *            member 要加入的值，
		 * @return 状态码 1成功，0已存在member的值
		 */
		public long zadd(String key, double score, String member) {
			Jedis jedis = getJedis();
			long s = jedis.zadd(key, score, member);
			returnJedis(jedis);
			return s;
		}

		public long zadd(String key, Map<String, Double> scoreMembers) {
			Jedis jedis = getJedis();
			long s = jedis.zadd(key, scoreMembers);
			returnJedis(jedis);
			return s;
		}

		/**
		 * 获取集合中元素的数量
		 *
		 * @param String
		 *            key
		 * @return 如果返回0则集合不存在
		 */
		public long zcard(String key) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			long len = sjedis.zcard(key);
			returnJedis(sjedis);
			return len;
		}

		/**
		 * 获取指定权重区间内集合的数量
		 *
		 * @param String
		 *            key
		 * @param double
		 *            min 最小排序位置
		 * @param double
		 *            max 最大排序位置
		 */
		public long zcount(String key, double min, double max) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			long len = sjedis.zcount(key, min, max);
			returnJedis(sjedis);
			return len;
		}

		/**
		 * 获得set的长度
		 *
		 * @param key
		 * @return
		 */
		public long zlength(String key) {
			long len = 0;
			Set<String> set = zrange(key, 0, -1);
			len = set.size();
			return len;
		}

		/**
		 * 权重增加给定值，如果给定的member已存在
		 *
		 * @param String
		 *            key
		 * @param double
		 *            score 要增的权重
		 * @param String
		 *            member 要插入的值
		 * @return 增后的权重
		 */
		public double zincrby(String key, double score, String member) {
			Jedis jedis = getJedis();
			double s = jedis.zincrby(key, score, member);
			returnJedis(jedis);
			return s;
		}

		/**
		 * 返回指定位置的集合元素,0为第一个元素，-1为最后一个元素
		 *
		 * @param String
		 *            key
		 * @param int
		 *            start 开始位置(包含)
		 * @param int
		 *            end 结束位置(包含)
		 * @return Set<String>
		 */
		public Set<String> zrange(String key, int start, int end) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			Set<String> set = sjedis.zrange(key, start, end);
			returnJedis(sjedis);
			return set;
		}

		/**
		 * 返回指定权重区间的元素集合
		 *
		 * @param String
		 *            key
		 * @param double
		 *            min 上限权重
		 * @param double
		 *            max 下限权重
		 * @return Set<String>
		 */
		public Set<String> zrangeByScore(String key, double min, double max) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			Set<String> set = sjedis.zrangeByScore(key, min, max);
			returnJedis(sjedis);
			return set;
		}

		public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			Set<String> set = sjedis.zrangeByScore(key, min, max, offset, count);
			returnJedis(sjedis);
			return set;
		}

		public Set<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			Set<String> set = sjedis.zrevrangeByScore(key, max, min, offset, count);
			returnJedis(sjedis);
			return set;
		}

		public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			Set<Tuple> set = sjedis.zrevrangeByScoreWithScores(key, max, min, offset, count);
			returnJedis(sjedis);
			return set;
		}

		/**
		 * 获取指定值在集合中的位置，集合排序从低到高
		 *
		 * @see zrevrank
		 * @param String
		 *            key
		 * @param String
		 *            member
		 * @return long 位置
		 */
		public long zrank(String key, String member) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			long index = sjedis.zrank(key, member);
			returnJedis(sjedis);
			return index;
		}

		/**
		 * 获取指定值在集合中的位置，集合排序从高到低
		 *
		 * @see zrank
		 * @param String
		 *            key
		 * @param String
		 *            member
		 * @return long 位置
		 */
		public long zrevrank(String key, String member) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			long index = sjedis.zrevrank(key, member);
			returnJedis(sjedis);
			return index;
		}

		/**
		 * 从集合中删除成员
		 *
		 * @param String
		 *            key
		 * @param String
		 *            member
		 * @return 返回1成功
		 */
		public long zrem(String key, String member) {
			Jedis jedis = getJedis();
			long s = jedis.zrem(key, member);
			returnJedis(jedis);
			return s;
		}

		public long zrem(String key, String... members) {
			Jedis jedis = getJedis();
			long s = jedis.zrem(key, members);
			returnJedis(jedis);
			return s;
		}

		/**
		 * 删除
		 *
		 * @param key
		 * @return
		 */
		public long zrem(String key) {
			Jedis jedis = getJedis();
			long s = jedis.del(key);
			returnJedis(jedis);
			return s;
		}

		/**
		 * 删除给定位置区间的元素
		 *
		 * @param String
		 *            key
		 * @param int
		 *            start 开始区间，从0开始(包含)
		 * @param int
		 *            end 结束区间,-1为最后一个元素(包含)
		 * @return 删除的数量
		 */
		public long zremrangeByRank(String key, int start, int end) {
			Jedis jedis = getJedis();
			long s = jedis.zremrangeByRank(key, start, end);
			returnJedis(jedis);
			return s;
		}

		/**
		 * 删除给定权重区间的元素
		 *
		 * @param String
		 *            key
		 * @param double
		 *            min 下限权重(包含)
		 * @param double
		 *            max 上限权重(包含)
		 * @return 删除的数量
		 */
		public long zremrangeByScore(String key, double min, double max) {
			Jedis jedis = getJedis();
			long s = jedis.zremrangeByScore(key, min, max);
			returnJedis(jedis);
			return s;
		}

		public long zremrangeByScore(String key, String min, String max, int offset, int count) {
			Jedis jedis = getJedis();
			long s = jedis.zremrangeByScore(key, min, max);
			returnJedis(jedis);
			return s;
		}

		/**
		 * 获取给定区间的元素，原始按照权重由高到低排序
		 *
		 * @param String
		 *            key
		 * @param int
		 *            start
		 * @param int
		 *            end
		 * @return Set<String>
		 */
		public Set<String> zrevrange(String key, int start, int end) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			Set<String> set = sjedis.zrevrange(key, start, end);
			returnJedis(sjedis);
			return set;
		}

		/**
		 * 获取给定值在集合中的权重
		 * 
		 * @param String
		 *            key
		 * @param memeber
		 * @return double 权重
		 */
		public double zscore(String key, String memebr) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			Double score = sjedis.zscore(key, memebr);
			returnJedis(sjedis);
			if (score != null)
				return score;
			return 0;
		}
	}

	// *******************************************Hash*******************************************//
	public class Hash {

		/**
		 * 从hash中删除指定的存储
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            fieid 存储的名字
		 * @return 状态码，1成功，0失败
		 */
		public long hdel(String key, String fieid) {
			Jedis jedis = getJedis();
			long s = jedis.hdel(key, fieid);
			returnJedis(jedis);
			return s;
		}

		public long hdel(String key, String... fieids) {
			Jedis jedis = getJedis();
			long s = jedis.hdel(key, fieids);
			returnJedis(jedis);
			return s;
		}

		public long hdel(String key) {
			Jedis jedis = getJedis();
			long s = jedis.del(key);
			returnJedis(jedis);
			return s;
		}

		/**
		 * 测试hash中指定的存储是否存在
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            fieid 存储的名字
		 * @return 1存在，0不存在
		 */
		public boolean hexists(String key, String fieid) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			boolean s = sjedis.hexists(key, fieid);
			returnJedis(sjedis);
			return s;
		}

		/**
		 * 返回hash中指定存储位置的值
		 *
		 * @param String
		 *            key
		 * @param String
		 *            fieid 存储的名字
		 * @return 存储对应的值
		 */
		public String hget(String key, String fieid) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			String s = sjedis.hget(key, fieid);
			returnJedis(sjedis);
			return s;
		}

		public byte[] hget(byte[] key, byte[] fieid) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			byte[] s = sjedis.hget(key, fieid);
			returnJedis(sjedis);
			return s;
		}

		/**
		 * 以Map的形式返回hash中的存储和值
		 * 
		 * @param String
		 *            key
		 * @return Map<Strinig,String>
		 */
		public Map<String, String> hgetAll(String key) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			Map<String, String> map = sjedis.hgetAll(key);
			returnJedis(sjedis);
			return map;
		}

		/**
		 * 添加一个对应关系
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            fieid
		 * @param String
		 *            value
		 * @return 状态码 1成功，0失败，fieid已存在将更新，也返回0
		 **/
		public long hset(String key, String fieid, String value) {
			Jedis jedis = getJedis();
			long s = jedis.hset(key, fieid, value);
			returnJedis(jedis);
			return s;
		}

		public long hset(String key, String fieid, byte[] value) {
			Jedis jedis = getJedis();
			long s = jedis.hset(key.getBytes(), fieid.getBytes(), value);
			returnJedis(jedis);
			return s;
		}

		/**
		 * 添加对应关系，只有在fieid不存在时才执行
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            fieid
		 * @param String
		 *            value
		 * @return 状态码 1成功，0失败fieid已存
		 **/
		public long hsetnx(String key, String fieid, String value) {
			Jedis jedis = getJedis();
			long s = jedis.hsetnx(key, fieid, value);
			returnJedis(jedis);
			return s;
		}

		/**
		 * 获取hash中value的集合
		 *
		 * @param String
		 *            key
		 * @return List<String>
		 */
		public List<String> hvals(String key) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			List<String> list = sjedis.hvals(key);
			returnJedis(sjedis);
			return list;
		}

		/**
		 * 在指定的存储位置加上指定的数字，存储位置的值必须可转为数字类型
		 *
		 * @param String
		 *            key
		 * @param String
		 *            fieid 存储位置
		 * @param String
		 *            long value 要增加的值,可以是负数
		 * @return 增加指定数字后，存储位置的值
		 */
		public long hincrby(String key, String fieid, long value) {
			Jedis jedis = getJedis();
			long s = jedis.hincrBy(key, fieid, value);
			returnJedis(jedis);
			return s;
		}

		/**
		 * 返回指定hash中的所有存储名字,类似Map中的keySet方法
		 *
		 * @param String
		 *            key
		 * @return Set<String> 存储名称的集合
		 */
		public Set<String> hkeys(String key) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			Set<String> set = sjedis.hkeys(key);
			returnJedis(sjedis);
			return set;
		}

		/**
		 * 获取hash中存储的个数，类似Map中size方法
		 *
		 * @param String
		 *            key
		 * @return long 存储的个数
		 */
		public long hlen(String key) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			long len = sjedis.hlen(key);
			returnJedis(sjedis);
			return len;
		}

		/**
		 * 根据多个key，获取对应的value，返回List,如果指定的key不存在,List对应位置为null
		 *
		 * @param String
		 *            key
		 * @param String
		 *            ... fieids 存储位置
		 * @return List<String>
		 */
		public List<String> hmget(String key, String... fieids) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			List<String> list = sjedis.hmget(key, fieids);
			returnJedis(sjedis);
			return list;
		}

		public List<byte[]> hmget(byte[] key, byte[]... fieids) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			List<byte[]> list = sjedis.hmget(key, fieids);
			returnJedis(sjedis);
			return list;
		}

		/**
		 * 添加对应关系，如果对应关系已存在，则覆盖
		 *
		 * @param Strin
		 *            key
		 * @param Map
		 *            <String,String> 对应关系
		 * @return 状态，成功返回OK
		 */
		public String hmset(String key, Map<String, String> map) {
			Jedis jedis = getJedis();
			String s = jedis.hmset(key, map);
			returnJedis(jedis);
			return s;
		}

		/**
		 * 添加对应关系，如果对应关系已存在，则覆盖
		 *
		 * @param Strin
		 *            key
		 * @param Map
		 *            <String,String> 对应关系
		 * @return 状态，成功返回OK
		 */
		public String hmset(byte[] key, Map<byte[], byte[]> map) {
			Jedis jedis = getJedis();
			String s = jedis.hmset(key, map);
			returnJedis(jedis);
			return s;
		}

	}

	// *******************************************Strings*******************************************//
	public class Strings {

		/**
		 * 根据key获取记录
		 * 
		 * @param String
		 *            key
		 * @return 值
		 */
		public String get(String key) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			String value = sjedis.get(key);
			returnJedis(sjedis);
			return value;
		}

		public String get(String key, int DBindex) {
			Jedis sjedis = getJedis(DBindex);
			String value = sjedis.get(key);
			returnJedis(sjedis);
			return value;
		}

		/**
		 * 根据key获取记录
		 * 
		 * @param byte[]
		 *            key
		 * @return 值
		 */
		public byte[] get(byte[] key) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			byte[] value = sjedis.get(key);
			returnJedis(sjedis);
			return value;
		}

		public byte[] get(byte[] key, int DBindex) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis(DBindex);
			byte[] value = sjedis.get(key);
			returnJedis(sjedis);
			return value;
		}

		/**
		 * 返回反序列化的对象
		 */
		public Object getObject(byte[] key) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			byte[] value = sjedis.get(key);
			Object o = SerializeUtil.unserizlize(value);
			returnJedis(sjedis);
			return o;
		}

		public Object getObject(byte[] key, int DBindex) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis(DBindex);
			byte[] value = sjedis.get(key);
			Object o = SerializeUtil.unserizlize(value);
			returnJedis(sjedis);
			return o;
		}

		/**
		 * 添加有过期时间的记录
		 *
		 * @param String
		 *            key
		 * @param int
		 *            seconds 过期时间，以秒为单位
		 * @param String
		 *            value
		 * @return String 操作状态
		 */
		public String setEx(String key, int seconds, String value) {
			Jedis jedis = getJedis();
			String str = jedis.setex(key, seconds, value);
			returnJedis(jedis);
			return str;
		}

		public String setEx(String key, int seconds, String value, int DBindex) {
			Jedis jedis = getJedis(DBindex);
			String str = jedis.setex(key, seconds, value);
			returnJedis(jedis);
			return str;
		}

		/**
		 * 添加有过期时间的记录
		 *
		 * @param String
		 *            key
		 * @param int
		 *            seconds 过期时间，以秒为单位
		 * @param String
		 *            value
		 * @return String 操作状态
		 */
		public String setEx(byte[] key, int seconds, byte[] value) {
			Jedis jedis = getJedis();
			String str = jedis.setex(key, seconds, value);
			returnJedis(jedis);
			return str;
		}

		public String setEx(byte[] key, int seconds, byte[] value, int DBindex) {
			Jedis jedis = getJedis(DBindex);
			String str = jedis.setex(key, seconds, value);
			returnJedis(jedis);
			return str;
		}

		/**
		 * 添加一条记录，仅当给定的key不存在时才插入
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            value
		 * @return long 状态码，1插入成功且key不存在，0未插入，key存在
		 */
		public long setnx(String key, String value) {
			Jedis jedis = getJedis();
			long str = jedis.setnx(key, value);
			returnJedis(jedis);
			return str;
		}

		public long setnx(String key, String value, int DBindex) {
			Jedis jedis = getJedis(DBindex);
			long str = jedis.setnx(key, value);
			returnJedis(jedis);
			return str;
		}

		/**
		 * 添加一条序列化对象的记录
		 */
		public <T extends Serializable> String setObject(String key, T value) {
			return set(SafeEncoder.encode(key), SerializeUtil.serialize(value));
		}

		public <T extends Serializable> String setObject(String key, T value, int DBindex) {
			return set(SafeEncoder.encode(key), SerializeUtil.serialize(value), DBindex);
		}

		/**
		 * 添加记录,如果记录已存在将覆盖原有的value
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            value
		 * @return 状态码
		 */
		public String set(String key, String value) {
			return set(SafeEncoder.encode(key), SafeEncoder.encode(value));
		}

		public String set(String key, String value, int DBindex) {
			return set(SafeEncoder.encode(key), SafeEncoder.encode(value), DBindex);
		}

		/**
		 * 添加记录,如果记录已存在将覆盖原有的value
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            value
		 * @return 状态码
		 */
		public String set(String key, byte[] value) {
			return set(SafeEncoder.encode(key), value);
		}

		public String set(String key, byte[] value, int DBindex) {
			return set(SafeEncoder.encode(key), value, DBindex);
		}

		/**
		 * 添加记录,如果记录已存在将覆盖原有的value
		 * 
		 * @param byte[]
		 *            key
		 * @param byte[]
		 *            value
		 * @return 状态码
		 */
		public String set(byte[] key, byte[] value) {
			Jedis jedis = getJedis();
			String status = jedis.set(key, value);
			returnJedis(jedis);
			return status;
		}

		public String set(byte[] key, byte[] value, int DBindex) {
			Jedis jedis = getJedis(DBindex);
			String status = jedis.set(key, value);
			returnJedis(jedis);
			return status;
		}

		/**
		 * 从指定位置开始插入数据，插入的数据会覆盖指定位置以后的数据<br/>
		 * 例:String str1="123456789";<br/>
		 * 对str1操作后setRange(key,4,0000)，str1="123400009";
		 * 
		 * @param String
		 *            key
		 * @param long
		 *            offset
		 * @param String
		 *            value
		 * @return long value的长度
		 */
		public long setRange(String key, long offset, String value) {
			Jedis jedis = getJedis();
			long len = jedis.setrange(key, offset, value);
			returnJedis(jedis);
			return len;
		}

		public long setRange(String key, long offset, String value, int DBindex) {
			Jedis jedis = getJedis(DBindex);
			long len = jedis.setrange(key, offset, value);
			returnJedis(jedis);
			return len;
		}

		/**
		 * 在指定的key中追加value
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            value
		 * @return long 追加后value的长度
		 **/
		public long append(String key, String value) {
			Jedis jedis = getJedis();
			long len = jedis.append(key, value);
			returnJedis(jedis);
			return len;
		}

		public long append(String key, String value, int DBindex) {
			Jedis jedis = getJedis(DBindex);
			long len = jedis.append(key, value);
			returnJedis(jedis);
			return len;
		}

		/**
		 * 将key对应的value减去指定的值，只有value可以转为数字时该方法才可用
		 *
		 * @param String
		 *            key
		 * @param long
		 *            number 要减去的值
		 * @return long 减指定值后的值
		 */
		public long decrBy(String key, long number) {
			Jedis jedis = getJedis();
			long len = jedis.decrBy(key, number);
			returnJedis(jedis);
			return len;
		}

		public long decrBy(String key, long number, int DBindex) {
			Jedis jedis = getJedis(DBindex);
			long len = jedis.decrBy(key, number);
			returnJedis(jedis);
			return len;
		}

		/**
		 * <b>可以作为获取唯一id的方法</b><br/>
		 * 将key对应的value加上指定的值，只有value可以转为数字时该方法才可用
		 * 
		 * @param String
		 *            key
		 * @param long
		 *            number 要减去的值
		 * @return long 相加后的值
		 */
		public long incrBy(String key, long number) {
			Jedis jedis = getJedis();
			long len = jedis.incrBy(key, number);
			returnJedis(jedis);
			return len;
		}

		public long incrBy(String key, long number, int DBindex) {
			Jedis jedis = getJedis(DBindex);
			long len = jedis.incrBy(key, number);
			returnJedis(jedis);
			return len;
		}

		/**
		 * 对指定key对应的value进行截取
		 * 
		 * @param String
		 *            key
		 * @param long
		 *            startOffset 开始位置(包含)
		 * @param long
		 *            endOffset 结束位置(包含)
		 * @return String 截取的值
		 */
		public String getrange(String key, long startOffset, long endOffset) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			String value = sjedis.getrange(key, startOffset, endOffset);
			returnJedis(sjedis);
			return value;
		}

		public String getrange(String key, long startOffset, long endOffset, int DBindex) {
			Jedis sjedis = getJedis(DBindex);
			String value = sjedis.getrange(key, startOffset, endOffset);
			returnJedis(sjedis);
			return value;
		}

		/**
		 * 获取并设置指定key对应的value<br/>
		 * 如果key存在返回之前的value,否则返回null
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            value
		 * @return String 原始value或null
		 */
		public String getSet(String key, String value) {
			Jedis jedis = getJedis();
			String str = jedis.getSet(key, value);
			returnJedis(jedis);
			return str;
		}

		public String getSet(String key, String value, int DBindex) {
			Jedis jedis = getJedis(DBindex);
			String str = jedis.getSet(key, value);
			returnJedis(jedis);
			return str;
		}

		/**
		 * 批量获取记录,如果指定的key不存在返回List的对应位置将是null
		 * 
		 * @param String
		 *            keys
		 * @return List<String> 值得集合
		 */
		public List<String> mget(String... keys) {
			Jedis jedis = getJedis();
			List<String> str = jedis.mget(keys);
			returnJedis(jedis);
			return str;
		}

		public List<String> mget(int DBindex, String... keys) {
			Jedis jedis = getJedis(DBindex);
			List<String> str = jedis.mget(keys);
			returnJedis(jedis);
			return str;
		}

		/**
		 * 批量存储记录
		 * 
		 * @param String
		 *            keysvalues 例:keysvalues="key1","value1","key2","value2";
		 * @return String 状态码
		 */
		public String mset(String... keysvalues) {
			Jedis jedis = getJedis();
			String str = jedis.mset(keysvalues);
			returnJedis(jedis);
			return str;
		}

		public String mset(int DBindex, String... keysvalues) {
			Jedis jedis = getJedis(DBindex);
			String str = jedis.mset(keysvalues);
			returnJedis(jedis);
			return str;
		}

		/**
		 * 获取key对应的值的长度
		 * 
		 * @param String
		 *            key
		 * @return value值得长度
		 */
		public long strlen(String key) {
			Jedis jedis = getJedis();
			long len = jedis.strlen(key);
			returnJedis(jedis);
			return len;
		}

		public long strlen(String key, int DBindex) {
			Jedis jedis = getJedis(DBindex);
			long len = jedis.strlen(key);
			returnJedis(jedis);
			return len;
		}
	}

	// *******************************************Lists*******************************************//
	public class Lists {

		/**
		 * List长度
		 * 
		 * @param String
		 *            key
		 * @return 长度
		 */
		public long llen(String key) {
			return llen(SafeEncoder.encode(key));
		}

		/**
		 * List长度
		 * 
		 * @param byte[]
		 *            key
		 * @return 长度
		 */
		public long llen(byte[] key) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			long count = sjedis.llen(key);
			returnJedis(sjedis);
			return count;
		}

		/**
		 * 覆盖操作,将覆盖List中指定位置的值
		 * 
		 * @param byte[]
		 *            key
		 * @param int
		 *            index 位置
		 * @param byte[]
		 *            value 值
		 * @return 状态码
		 */
		public String lset(byte[] key, int index, byte[] value) {
			Jedis jedis = getJedis();
			String status = jedis.lset(key, index, value);
			returnJedis(jedis);
			return status;
		}

		/**
		 * 覆盖操作,将覆盖List中指定位置的值
		 * 
		 * @param key
		 * @param int
		 *            index 位置
		 * @param String
		 *            value 值
		 * @return 状态码
		 */
		public String lset(String key, int index, String value) {
			return lset(SafeEncoder.encode(key), index, SafeEncoder.encode(value));
		}

		/**
		 * 在value的相对位置插入记录
		 * 
		 * @param key
		 * @param LIST_POSITION
		 *            前面插入或后面插入
		 * @param String
		 *            pivot 相对位置的内容
		 * @param String
		 *            value 插入的内容
		 * @return 记录总数
		 */
		public long linsert(String key, LIST_POSITION where, String pivot, String value) {
			return linsert(SafeEncoder.encode(key), where, SafeEncoder.encode(pivot), SafeEncoder.encode(value));
		}

		/**
		 * 在指定位置插入记录
		 * 
		 * @param String
		 *            key
		 * @param LIST_POSITION
		 *            前面插入或后面插入
		 * @param byte[]
		 *            pivot 相对位置的内容
		 * @param byte[]
		 *            value 插入的内容
		 * @return 记录总数
		 */
		public long linsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value) {
			Jedis jedis = getJedis();
			long count = jedis.linsert(key, where, pivot, value);
			returnJedis(jedis);
			return count;
		}

		/**
		 * 获取List中指定位置的值
		 * 
		 * @param String
		 *            key
		 * @param int
		 *            index 位置
		 * @return 值
		 **/
		public String lindex(String key, int index) {
			return SafeEncoder.encode(lindex(SafeEncoder.encode(key), index));
		}

		/**
		 * 获取List中指定位置的值
		 * 
		 * @param byte[]
		 *            key
		 * @param int
		 *            index 位置
		 * @return 值
		 **/
		public byte[] lindex(byte[] key, int index) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			byte[] value = sjedis.lindex(key, index);
			returnJedis(sjedis);
			return value;
		}

		/**
		 * 将List中的第一条记录移出List
		 * 
		 * @param String
		 *            key
		 * @return 移出的记录
		 */
		public String lpop(String key) {
			return SafeEncoder.encode(lpop(SafeEncoder.encode(key)));
		}

		/**
		 * 将List中的第一条记录移出List
		 * 
		 * @param byte[]
		 *            key
		 * @return 移出的记录
		 */
		public byte[] lpop(byte[] key) {
			Jedis jedis = getJedis();
			byte[] value = jedis.lpop(key);
			returnJedis(jedis);
			return value;
		}

		/**
		 * 将List中最后第一条记录移出List
		 *
		 * @param byte[]
		 *            key
		 * @return 移出的记录
		 */
		public String rpop(String key) {
			Jedis jedis = getJedis();
			String value = jedis.rpop(key);
			returnJedis(jedis);
			return value;
		}

		/**
		 * 向List尾部追加记录
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            value
		 * @return 记录总数
		 */
		public long lpush(String key, String value) {
			return lpush(SafeEncoder.encode(key), SafeEncoder.encode(value));
		}

		/**
		 * 向List头部追加记录
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            value
		 * @return 记录总数
		 */
		public long rpush(String key, String value) {
			Jedis jedis = getJedis();
			long count = jedis.rpush(key, value);
			returnJedis(jedis);
			return count;
		}

		/**
		 * 向List头部追加记录，带数据库
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            value
		 * @return 记录总数
		 */
		public long rpush(String key, String value, int DBindex) {
			Jedis jedis = getJedis(DBindex);
			long count = jedis.rpush(key, value);
			returnJedis(jedis);
			return count;
		}

		/**
		 * 向List尾部追加记录，带数据库
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            value
		 * @return 记录总数
		 */
		public long lpush(String key, String value, int DBindex) {
			Jedis jedis = getJedis(DBindex);
			long count = jedis.lpush(SafeEncoder.encode(key), SafeEncoder.encode(value));
			returnJedis(jedis);
			return count;
		}

		/**
		 * 向List头部追加记录
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            value
		 * @return 记录总数
		 */
		public long rpush(byte[] key, byte[] value) {
			Jedis jedis = getJedis();
			long count = jedis.rpush(key, value);
			returnJedis(jedis);
			return count;
		}

		/**
		 * 向List中追加记录
		 * 
		 * @param byte[]
		 *            key
		 * @param byte[]
		 *            value
		 * @return 记录总数
		 */
		public long lpush(byte[] key, byte[] value) {
			Jedis jedis = getJedis();
			long count = jedis.lpush(key, value);
			returnJedis(jedis);
			return count;
		}

		/**
		 * 获取指定范围的记录，可以做为分页使用
		 * 
		 * @param String
		 *            key
		 * @param long
		 *            start
		 * @param long
		 *            end
		 * @return List
		 */
		public List<String> lrange(String key, long start, long end) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			List<String> list = sjedis.lrange(key, start, end);
			returnJedis(sjedis);
			return list;
		}

		/**
		 * 获取指定范围的记录，可以做为分页使用
		 * 
		 * @param String
		 *            key
		 * @param long
		 *            start
		 * @param long
		 *            end
		 * @return List
		 */
		public List<String> lrange(String key, long start, long end, int DBindex) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis(DBindex);
			List<String> list = sjedis.lrange(key, start, end);
			returnJedis(sjedis);
			return list;
		}

		/**
		 * 获取指定范围的记录，可以做为分页使用
		 * 
		 * @param byte[]
		 *            key
		 * @param int
		 *            start
		 * @param int
		 *            end 如果为负数，则尾部开始计算
		 * @return List
		 */
		public List<byte[]> lrange(byte[] key, int start, int end) {
			// ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = getJedis();
			List<byte[]> list = sjedis.lrange(key, start, end);
			returnJedis(sjedis);
			return list;
		}

		/**
		 * 删除List中c条记录，被删除的记录值为value
		 * 
		 * @param byte[]
		 *            key
		 * @param int
		 *            c 要删除的数量，如果为负数则从List的尾部检查并删除符合的记录
		 * @param byte[]
		 *            value 要匹配的值
		 * @return 删除后的List中的记录数
		 */
		public long lrem(byte[] key, int c, byte[] value) {
			Jedis jedis = getJedis();
			long count = jedis.lrem(key, c, value);
			returnJedis(jedis);
			return count;
		}

		/**
		 * 删除List中c条记录，被删除的记录值为value
		 * 
		 * @param String
		 *            key
		 * @param int
		 *            c 要删除的数量，如果为负数则从List的尾部检查并删除符合的记录
		 * @param String
		 *            value 要匹配的值
		 * @return 删除后的List中的记录数
		 */
		public long lrem(String key, int c, String value) {
			return lrem(SafeEncoder.encode(key), c, SafeEncoder.encode(value));
		}

		/**
		 * 删除List中c条记录，被删除的记录值为value,带数据库编号
		 * 
		 * @param String
		 *            key
		 * @param int
		 *            c 要删除的数量，如果为负数则从List的尾部检查并删除符合的记录
		 * @param String
		 *            value 要匹配的值
		 * @return 删除后的List中的记录数
		 */
		public long lrem(String key, int c, String value, int DBindex) {
			Jedis jedis = getJedis(DBindex);
			long count = jedis.lrem(SafeEncoder.encode(key), c, SafeEncoder.encode(value));
			returnJedis(jedis);
			return count;
		}

		/**
		 * 算是删除吧，只保留start与end之间的记录
		 * 
		 * @param byte[]
		 *            key
		 * @param int
		 *            start 记录的开始位置(0表示第一条记录)
		 * @param int
		 *            end 记录的结束位置（如果为-1则表示最后一个，-2，-3以此类推）
		 * @return 执行状态码
		 */
		public String ltrim(byte[] key, int start, int end) {
			Jedis jedis = getJedis();
			String str = jedis.ltrim(key, start, end);
			returnJedis(jedis);
			return str;
		}

		/**
		 * 算是删除吧，只保留start与end之间的记录
		 * 
		 * @param String
		 *            key
		 * @param int
		 *            start 记录的开始位置(0表示第一条记录)
		 * @param int
		 *            end 记录的结束位置（如果为-1则表示最后一个，-2，-3以此类推）
		 * @return 执行状态码
		 */
		public String ltrim(String key, int start, int end) {
			return ltrim(SafeEncoder.encode(key), start, end);
		}
	}

}
