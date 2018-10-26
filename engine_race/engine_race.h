#ifndef ENGINE_RACE_ENGINE_RACE_H_
#define ENGINE_RACE_ENGINE_RACE_H_
#include "include/engine.h"
#include "write_ahead_log.h"
#include "SSTables.h"
#include <stdint.h>
#include <pthread.h>
#include <unistd.h>

namespace polar_race {
	class EngineRace : public Engine {
	public:
		static RetCode Open(const std::string& name, Engine** eptr);

		explicit EngineRace(const std::string& dir,int size=200) :dir_path_(dir),data_path_(dir+"/from"),log_path_(dir + "/redolog"),log_store_(dir+"/redolog" ), sstables_(dir+"/from",dir+"/to",size),memtable_max_size_(size){
			memtable_.clear();
		}

		~EngineRace();

		RetCode Write(const PolarString& key,
			const PolarString& value) override;

		RetCode Read(const PolarString& key,
			std::string* value) override;

		/*
		 * NOTICE: Implement 'Range' in quarter-final,
		 *         you can skip it in preliminary.
		 */

		RetCode Range(const PolarString& lower,
			const PolarString& upper,
			Visitor &visitor) override;
	private:
		pthread_mutex_t mut_;
		std::string dir_path_;
		std::string data_path_;
		std::string log_path_;
		LogStore log_store_;
		SSTables sstables_;
		std::map<std::string, LogItem> memtable_;
		unsigned int memtable_max_size_;
	};

}

#endif