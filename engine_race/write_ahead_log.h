#ifndef ENGINE_WRITE_AHEAD_LOG_H_
#define ENGINE_WRITE_AHEAD_LOG_H_
#include <string>
#include <map>
#include <fstream>
#include <stdint.h>
#include <pthread.h>
#include <unistd.h>
#include "include/polar_string.h"
#include "include/engine.h"
namespace polar_race {

	class LogItem {
	public:
		explicit LogItem(const std::string item = "",unsigned int keylength = 8) : content_(item),key_length_(keylength) { }
		PolarString GetKey() const;
		PolarString GetValue() const;
		std::string GetValueStr() const;
		void SetItem(PolarString key, PolarString value);
		void SetItem(std::string item);
		std::string GetItem() const;
	private:
		std::string content_;
		unsigned int key_length_;
	};

	class LogStore {
	public:
		explicit LogStore(const std::string log) : log_path_(log) { }
		~LogStore() { 
			input_.close();
			output_.close();
		}
		RetCode Init();
		RetCode Read(std::map<std::string,LogItem>* memtable_ptr);
		RetCode Append(const LogItem &item);
		RetCode Clear();
	private:
		pthread_mutex_t mu_;								//多线程的信号量
		std::string log_path_;								//存储log文件的路径
		std::ifstream input_;								//读文件到内存的文件流对象
		std::ofstream output_;								//写文件到磁盘的文件流对象
		
	};

	inline bool FileExists(const std::string& path) {
		return access(path.c_str(), F_OK) == 0;
	}
}

#endif
