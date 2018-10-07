package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.bitcask.BitCask;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;

public class EngineRace extends AbstractEngine {

	private BitCask bitCask;

	@Override
	public void open(String path) throws EngineException {
		bitCask = new BitCask(path);
	}
	
	@Override
	public void write(byte[] key, byte[] value) throws EngineException {

	}
	
	@Override
	public byte[] read(byte[] key) throws EngineException {
		byte[] value = null;
		
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
