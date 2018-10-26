#ifndef ENGINE_SSTABLES_H_
#define ENGINE_SSTABLES_H_
#include <sys/stat.h>
#include <string>
#include <fstream>
#include <vector>
#include <unistd.h>
#include <cstdio>
#include "include/engine.h"
#include "write_ahead_log.h"
namespace polar_race {

	inline bool DirExists(const std::string& path) {
		return access(path.c_str(), F_OK) == 0;
	}

	class SSTables {
	public:
		SSTables(const std::string dir, const std::string temp, int len = 20480) : dir_path_(dir), temp_path_(temp),length_(len) {
		}
		~SSTables() { }
		RetCode Init();
		int GetDirFiles(const std::string& dir, std::vector<std::string>* result);
		RetCode Merge();
		RetCode Write();
		RetCode Read(const std::string& key, std::string* value,int value_length);
	private:
		std::string dir_path_;		//存储数据库文件的文件夹目录
		std::string temp_path_;		//存储零时合并的merge文件的文件夹目录
		std::map<std::string,LogItem> merge;		//merge的临时文件
		unsigned int length_;
	};
}
#endif
