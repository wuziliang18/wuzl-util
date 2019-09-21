package org.wuzl.util.redis;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class SerializeUtil {
	private static final Logger logger = LoggerFactory.getLogger(SerializeUtil.class);

	public static <T> T parseObject(byte[] bytes, Class<T> clazz) {
		if (bytes == null || bytes.length == 0) {
			return null;
		}
		try {
			return JSONObject.parseObject(bytes, clazz);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("数据解析失败");
		}
	}

	public static <T> T parseObjectByString(String value, Class<T> clazz) {
		if (value == null || value.length() == 0) {
			return null;
		}
		try {
			return JSONObject.parseObject(value, clazz);
		} catch (Exception e) {
			throw new RuntimeException("数据解析失败");
		}
	}

	public static byte[] toBytes(Object obj) {
		if (obj == null) {
			return null;
		}
		return JSON.toJSONString(obj).getBytes(Charset.forName("utf-8"));
	}

	/**
	 * 序列化对象
	 * 
	 * @param obj
	 * @return
	 */
	public static byte[] serialize(Object obj) {
		if (obj == null) {
			return null;
		}
		byte[] byt = null;
		ObjectOutputStream oos = null;
		ByteArrayOutputStream bos = null;
		try {
			bos = new ByteArrayOutputStream();
			oos = new ObjectOutputStream(bos);
			oos.writeObject(obj);
			byt = bos.toByteArray();
		} catch (IOException e) {
			logger.error("序列化对象失败：", e);
		} finally {
			if (oos != null) {
				try {
					oos.close();
				} catch (IOException e) {
					logger.error("序列化对象失败：", e);
				}
			}
		}
		return byt;
	}

	/**
	 * 反序列化对象
	 * 
	 * @param byt
	 * @return
	 */
	public static Object unserizlize(byte[] byt) {
		if (byt == null) {
			return null;
		}
		Object obj = null;
		ObjectInputStream ois = null;
		ByteArrayInputStream bis = null;
		try {
			bis = new ByteArrayInputStream(byt);
			ois = new ObjectInputStream(bis);
			obj = ois.readObject();
		} catch (Exception e) {
			logger.error("反序列化对象失败：", e);
		} finally {
			if (ois != null) {
				try {
					ois.close();
				} catch (IOException e) {
					logger.error("反序列化对象失败：", e);
				}
			}
		}
		return obj;
	}
}
