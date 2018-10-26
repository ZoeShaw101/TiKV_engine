#include <sstream>
#include <ctime>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <fstream>

#include "engine_race.h"
#include "write_ahead_log.h"
namespace polar_race {
	RetCode Engine::Open(const std::string& name, Engine** eptr) {
		return EngineRace::Open(name, eptr);
	}

	Engine::~Engine() {
	}

	RetCode EngineRace::Open(const std::string& name, Engine** eptr) {
		//创建初始文件夹
		if (!DirExists(name) && 0 != mkdir(name.c_str(), 0755)) {
			return kIOError;
		}
		*eptr = NULL;
		EngineRace *engine_race = new EngineRace(name);												//创建
		RetCode ret = engine_race->log_store_.Init();												//初始化
		if (ret != kSucc) {
			delete engine_race;
			return ret;
		}
		engine_race->memtable_.clear();
		ret = engine_race->log_store_.Read(&(engine_race->memtable_));								//读取redolog
		if (ret != kSucc) {
			delete engine_race;
			return ret;
		}
		ret = engine_race->sstables_.Init();														//sstables_初始化
		if (ret != kSucc) {
			delete engine_race;
			return ret;
		}
		*eptr = engine_race;
		return kSucc;
	}

	EngineRace::~EngineRace() {			//析构函数
	}

	RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
		LogItem temp;
		temp.SetItem(key, value);
		RetCode ret = log_store_.Append(temp);
		if (ret == kSucc) {
			memtable_[key.ToString()] = temp;
		}
		pthread_mutex_lock(&mut_);
		if (memtable_.size() == memtable_max_size_) {
			//如果内存满了就去写文件
			std::string result;
			for (auto it : memtable_) {
				result.append(it.second.GetItem());
				result.append("\n");
			}
			std::stringstream ss;
			auto t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
			ss << std::put_time(std::localtime(&t), "%Y%m%d%H%M%S");
			std::string filename = ss.str();						//生成文件名
			filename = data_path_ + "/" + filename + "_" + memtable_.begin()->first;
			std::ofstream output;
			output.open(filename);
			output << result;
			output.close();
			//写完就清空memtable的内容
			memtable_.clear();
			//并且清空redologo的内容
			std::ofstream out_put;
			out_put.open(log_path_, std::ofstream::trunc);
			if (!out_put) { return kIOError; }
			out_put.close();
			//然后merge，目前先这样，但是这样不太好(写文件时候要merge，有点频繁)
			sstables_.Merge();
		}
		pthread_mutex_unlock(&mut_);
		return ret;
	}

	RetCode EngineRace::Read(const PolarString& key, std::string* value) {
		auto it = memtable_.find(key.ToString());
		if (it != memtable_.end()) {
			value->clear();
			//PolarString tempPolarStr(it->second.GetValue());
			std::string temp = it->second.GetValueStr();
			//std::string temp = it->second.GetValue().ToString();
			*value = temp;
			return kSucc;
		}
		else {
			//去文件中找
			RetCode ret = sstables_.Read(key.ToString(), value, 4096);
			return ret;
		}
	}

	RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,Visitor &visitor) {
		return kSucc;
	}
}
