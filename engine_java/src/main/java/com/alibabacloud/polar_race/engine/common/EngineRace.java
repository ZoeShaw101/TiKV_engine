package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.bitcask.BitCask;
import com.alibabacloud.polar_race.engine.core.LSMDB;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.lsmtree.LSMTree;

import org.apache.log4j.Logger;

public class EngineRace extends AbstractEngine {

	private Logger logger = Logger.getLogger(EngineRace.class);
	private LSMDB db;

	@Override
	public void open(String path) throws EngineException {
		db = new LSMDB(path);
	}
	
	@Override
	public void write(byte[] key, byte[] value) throws EngineException {
		db.put(key, value);
	}
	
	@Override
	public byte[] read(byte[] key) throws EngineException {
		return db.get(key);
	}
	
	@Override
	public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
	}
	
	@Override
	public void close() {
		try {
			db.close();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("关闭数据库出错！" + e);
		}
	}

}
