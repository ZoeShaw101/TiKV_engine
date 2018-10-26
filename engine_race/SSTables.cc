#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <fstream>
#include <map>
#include <algorithm>
#include <ctime>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <utility>
#include <cstdio>
#include <fcntl.h>
#include <iostream>

#include <unistd.h>

#include "SSTables.h"
#include "write_ahead_log.h"
namespace polar_race {
	RetCode SSTables::Init() {
		merge.clear();
		if (!DirExists(dir_path_) && 0 != mkdir(dir_path_.c_str(), 0755)) {
			return kIOError;
		}
		if (!DirExists(temp_path_) && 0 != mkdir(temp_path_.c_str(), 0755)) {
			return kIOError;
		}
		std::vector<std::string> temp_files_name;								//下面是清空temp_path_文件夹
		if (0 != GetDirFiles(temp_path_, &temp_files_name)) {
			return kIOError;
		}
		if (temp_files_name.empty()) {											//已经是空的直接返回
			return kSucc;
		}
		for (auto file_name : temp_files_name) {
			std::string file_path = temp_path_ + "/" + file_name;
			remove(file_path.c_str());
		}
		return kSucc;
	}

	int SSTables::GetDirFiles(const std::string& dir, std::vector<std::string>* result) {
		int res = 0;
		result->clear();
		DIR* d = opendir(dir.c_str());
		if (d == NULL) {
			return errno;
		}
		struct dirent* entry;
		while ((entry = readdir(d)) != NULL) {
			if (strcmp(entry->d_name, "..") == 0 || strcmp(entry->d_name, ".") == 0) {
				continue;
			}
			result->push_back(entry->d_name);
		}
		closedir(d);
		return res;
	}

	RetCode SSTables::Merge() {
		std::vector<std::string> files_name;
		if (0 != GetDirFiles(dir_path_, &files_name)) {					//获取所有的数据文件
			return kIOError;
		}
		if (files_name.size() <= 1) {									//如果文件数量小于等于1就直接返回即可
			return kSucc;
		}
		sort(files_name.begin(),files_name.end());						//对这些数据文件按照文件名排序
		std::vector<std::ifstream *> files_ptr;
		for (unsigned int i = 0; i < files_name.size(); ++i) {			//这些指针的顺序是按照所指文件名排序的
			std::ifstream* ptr = new std::ifstream(dir_path_ + "/" + files_name[i]);
			files_ptr.push_back(ptr);
		}
		std::map<std::string, std::map<int , LogItem>> temp;

		for (unsigned int i = 0; i < files_ptr.size(); ++i) {			//先构造初始的temp缓存	
			if (files_ptr[i]->is_open()) {
				std::string line_temp;
				if (getline(*files_ptr[i], line_temp)) {				//[]优先级高于*，所以不用加括号
					if (line_temp.empty()) {
						files_ptr[i]->close();
						continue;
					}
					LogItem data_temp(line_temp);						//文件不是空的
					temp[data_temp.GetKey().ToString()].insert(std::pair<int, LogItem>(i, data_temp));
				}
				else {													//文件是空的就关闭吧
					files_ptr[i]->close();
				}
			}
		}
		while (true) {
			if (!temp.empty()) {
				merge[temp.begin()->first] = (--(temp.begin()->second.end()))->second;		//加入merge
				for (auto it : temp.begin()->second) {										//读新值进入temp
					std::string line_temp;
					if (getline(*files_ptr[it.first], line_temp)) {
						if (line_temp.empty()) {
							files_ptr[it.first]->close();
							continue;
						}
						LogItem data_temp(line_temp);
						temp[data_temp.GetKey().ToString()].insert(std::pair<int, LogItem>(it.first, data_temp));
					}
					else {
						files_ptr[it.first]->close();
					}
				}
				temp.erase(temp.begin());													//删除已经排序好的旧值
				if (merge.size() == length_) {												//如果merge满了就写文件
					Write();
					merge.clear();
				}
			}
			else {
				if (!merge.empty()) {														//最后把内存留存的条目存储到文件
					Write();
					merge.clear();
				}
				break;																		//merge完毕跳出循环
			}
		}
		for (unsigned int i = 0; i < files_ptr.size(); ++i) {								//释放new的内存
			delete files_ptr[i];
		}
		rename(temp_path_.c_str(),"middle");
		rename(dir_path_.c_str(), temp_path_.c_str());
		rename("middle",dir_path_.c_str());

		std::vector<std::string> temp_files_name;								//下面是清空temp_path_文件夹
		if (0 != GetDirFiles(temp_path_, &temp_files_name)) {
			return kIOError;
		}
		if (temp_files_name.empty()) {											//已经是空的直接返回
			return kSucc;
		}
		for (auto file_name : temp_files_name) {
			std::string file_path = temp_path_ + "/" + file_name;
			remove(file_path.c_str());
		}
		return kSucc;
	}

	RetCode SSTables::Write() {
		if (merge.empty()) {
			return kNotSupported;	//不支持写空merge
		}
		std::string result;
		for (auto it : merge) {
			result.append(it.second.GetItem());
			result.append("\n");
		}
		std::stringstream ss;
		auto t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		ss << std::put_time(std::localtime(&t), "%Y%m%d%H%M%S");
		std::string filename = ss.str();						//生成文件名
		filename = temp_path_ + "/" + filename + "_" + merge.begin()->first;
		std::ofstream output(filename);
		output << result;
		output.close();
		sleep(1);
		return kSucc;
	}

	RetCode SSTables::Read(const std::string& key, std::string* value,int value_length) {
		//从文件中读取想要的值
		std::vector<std::string> files_name;
		if (0 != GetDirFiles(dir_path_, &files_name)) {					//获取所有的数据文件
			return kIOError;
		}
		if (files_name.size() <= 0) {									//如果文件数量小于等于0就直接返回即可
			return kNotFound;
		}
		if (files_name.size() == 1 ) {									//仅有一个文件就找这一个文件
			if (key < files_name[0].substr(15)) {						//找不到
				return kNotFound;
			}
			else {
				//文件内二分查找
				std::ifstream input;
				input.open(dir_path_ + "/" + files_name[0]);
				input.seekg(0, std::fstream::end);
				unsigned int n = input.tellg();
				unsigned int row_length = key.size() + value_length + 2;		//加2是因为换行符的存在
				unsigned int start = 0;
				unsigned int end = n / row_length;
				std::string res = "";
				unsigned int mid;
				while (start <= end) {
					mid = start + (end - start) / 2;
					input.seekg(mid*row_length, std::fstream::beg);
					std::string temp;
					getline(input, temp);
					if (temp.substr(0, key.size()) < key) {
						start = mid + 1;
					}
					else if (temp.substr(0, key.size()) > key) {
						end = mid - 1;
					}
					else {
						res = temp.substr(key.size());		//获取value
						break;
					}
				}
				*value = res;
				if (res == "") {
					return kNotFound;
				}
				return kSucc;
			}
		}
		sort(files_name.begin(), files_name.end());						//文件数目大于一就得排序之后找
		//二分定位要查找的文件
		//end就是我们想要定位的文件
		int file_start = 0;
		int file_end = files_name.size() - 1;
		int file_mid;
		while (file_start <= file_end) {
			file_mid = file_start + (file_end - file_start) / 2;
			if (files_name[file_mid].substr(15) <= key) {
				file_start = file_mid +1;
			}
			else {
				file_end = file_mid - 1;
			}
		}
		//文件内二分查找
		std::ifstream input;
		input.open(dir_path_+"/"+files_name[file_end]);
		input.seekg(0, std::fstream::end);
		unsigned int n = input.tellg();
		unsigned int row_length = key.size() + value_length + 1;		//加1是因为换行符的存在(实测是加1)
		unsigned int start = 0;
		unsigned int end = n / row_length;
		std::string res="";
		unsigned int mid;
		while (start <= end) {
			mid = start + (end - start) / 2;
			input.seekg(mid*row_length, std::fstream::beg);
			std::string temp;
			getline(input, temp);
			if (temp.substr(0, key.size() )< key) {
				start = mid + 1;
			}
			else if (temp.substr(0, key.size()) > key) {
				end = mid - 1;
			}
			else {
				res = temp.substr(key.size());		//获取value
				break;
			}
		}
		*value = res;
		if (res == "") {
			return kNotFound;
		}
		return kSucc;
	}
}