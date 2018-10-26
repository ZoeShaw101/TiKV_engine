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
		//������ʼ�ļ���
		if (!DirExists(name) && 0 != mkdir(name.c_str(), 0755)) {
			return kIOError;
		}
		*eptr = NULL;
		EngineRace *engine_race = new EngineRace(name);												//����
		RetCode ret = engine_race->log_store_.Init();												//��ʼ��
		if (ret != kSucc) {
			delete engine_race;
			return ret;
		}
		engine_race->memtable_.clear();
		ret = engine_race->log_store_.Read(&(engine_race->memtable_));								//��ȡredolog
		if (ret != kSucc) {
			delete engine_race;
			return ret;
		}
		ret = engine_race->sstables_.Init();														//sstables_��ʼ��
		if (ret != kSucc) {
			delete engine_race;
			return ret;
		}
		*eptr = engine_race;
		return kSucc;
	}

	EngineRace::~EngineRace() {			//��������
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
			//����ڴ����˾�ȥд�ļ�
			std::string result;
			for (auto it : memtable_) {
				result.append(it.second.GetItem());
				result.append("\n");
			}
			// std::stringstream ss;
			// auto t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
			// ss << std::put_time(std::localtime(&t), "%Y%m%d%H%M%S");
			// std::string filename = ss.str();						//�����ļ���
			time_t t = time(0);
			char tmpBuf[255];
			strftime(tmpBuf, 255, "%Y%m%d%H%M%S", localtime(&t)); //format date and time. 
			std::string filename(tmpBuf);
			filename = data_path_ + "/" + filename + "_" + memtable_.begin()->first;
			std::ofstream output;
			output.open(filename);
			output << result;
			output.close();
			//д������memtable������
			memtable_.clear();
			//�������redologo������
			std::ofstream out_put;
			out_put.open(log_path_, std::ofstream::trunc);
			if (!out_put) { return kIOError; }
			out_put.close();
			//Ȼ��merge��Ŀǰ������������������̫��(д�ļ�ʱ��Ҫmerge���е�Ƶ��)
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
			//ȥ�ļ�����
			RetCode ret = sstables_.Read(key.ToString(), value, 4096);
			return ret;
		}
	}

	RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,Visitor &visitor) {
		return kSucc;
	}
}
