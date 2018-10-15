package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.bitcask.BitCask;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.common.lsmtree.LSMTree;
import com.alibabacloud.polar_race.engine.common.utils.Serialization;
import org.apache.log4j.Logger;


public class EngineRace extends AbstractEngine {

	private Logger logger = Logger.getLogger(EngineRace.class);
	private BitCask<byte[]> bitCask;
	private LSMTree lsmTree;

	@Override
	public void open(String path) throws EngineException {
		bitCask = new BitCask<byte[]>(path);
	}
	
	@Override
	public void write(byte[] key, byte[] value) throws EngineException {
		String strKey = new String(key);
		try {
			if (bitCask != null) {
				bitCask.put(strKey, value);
			}
		} catch (Exception e) {
			logger.error("写入k/v数据出错：", e);
		}
	}
	
	@Override
	public byte[] read(byte[] key) throws EngineException {
		String strKey = new String(key);
		byte[] value = null;
		try {
			if (bitCask != null) {
				value = bitCask.get(strKey);
				if (value == null) logger.warn("要查找的key记录不存在");
			}
		} catch (Exception e) {
			logger.error("获取value数据出错：", e);
		}
		return value;
	}
	
	@Override
	public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
	}
	
	@Override
	public void close() {
		bitCask.close();
	}

}
