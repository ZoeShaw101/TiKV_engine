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
		std::vector<std::string> temp_files_name;								//���������temp_path_�ļ���
		if (0 != GetDirFiles(temp_path_, &temp_files_name)) {
			return kIOError;
		}
		if (temp_files_name.empty()) {											//�Ѿ��ǿյ�ֱ�ӷ���
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
		if (0 != GetDirFiles(dir_path_, &files_name)) {					//��ȡ���е������ļ�
			return kIOError;
		}
		if (files_name.size() <= 1) {									//����ļ�����С�ڵ���1��ֱ�ӷ��ؼ���
			return kSucc;
		}
		sort(files_name.begin(),files_name.end());						//����Щ�����ļ������ļ�������
		std::vector<std::ifstream *> files_ptr;
		for (unsigned int i = 0; i < files_name.size(); ++i) {			//��Щָ���˳���ǰ�����ָ�ļ��������
			std::ifstream* ptr = new std::ifstream(dir_path_ + "/" + files_name[i]);
			files_ptr.push_back(ptr);
		}
		std::map<std::string, std::map<int , LogItem>> temp;

		for (unsigned int i = 0; i < files_ptr.size(); ++i) {			//�ȹ����ʼ��temp����	
			if (files_ptr[i]->is_open()) {
				std::string line_temp;
				if (getline(*files_ptr[i], line_temp)) {				//[]���ȼ�����*�����Բ��ü�����
					if (line_temp.empty()) {
						files_ptr[i]->close();
						continue;
					}
					LogItem data_temp(line_temp);						//�ļ����ǿյ�
					temp[data_temp.GetKey().ToString()].insert(std::pair<int, LogItem>(i, data_temp));
				}
				else {													//�ļ��ǿյľ͹رհ�
					files_ptr[i]->close();
				}
			}
		}
		while (true) {
			if (!temp.empty()) {
				merge[temp.begin()->first] = (--(temp.begin()->second.end()))->second;		//����merge
				for (auto it : temp.begin()->second) {										//����ֵ����temp
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
				temp.erase(temp.begin());													//ɾ���Ѿ�����õľ�ֵ
				if (merge.size() == length_) {												//���merge���˾�д�ļ�
					Write();
					merge.clear();
				}
			}
			else {
				if (!merge.empty()) {														//�����ڴ�������Ŀ�洢���ļ�
					Write();
					merge.clear();
				}
				break;																		//merge�������ѭ��
			}
		}
		for (unsigned int i = 0; i < files_ptr.size(); ++i) {								//�ͷ�new���ڴ�
			delete files_ptr[i];
		}
		rename(temp_path_.c_str(),"middle");
		rename(dir_path_.c_str(), temp_path_.c_str());
		rename("middle",dir_path_.c_str());

		std::vector<std::string> temp_files_name;								//���������temp_path_�ļ���
		if (0 != GetDirFiles(temp_path_, &temp_files_name)) {
			return kIOError;
		}
		if (temp_files_name.empty()) {											//�Ѿ��ǿյ�ֱ�ӷ���
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
			return kNotSupported;	//��֧��д��merge
		}
		std::string result;
		for (auto it : merge) {
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
		filename = temp_path_ + "/" + filename + "_" + merge.begin()->first;
		std::ofstream output(filename);
		output << result;
		output.close();
		return kSucc;
	}

	RetCode SSTables::Read(const std::string& key, std::string* value,int value_length) {
		//���ļ��ж�ȡ��Ҫ��ֵ
		std::vector<std::string> files_name;
		if (0 != GetDirFiles(dir_path_, &files_name)) {					//��ȡ���е������ļ�
			return kIOError;
		}
		if (files_name.size() <= 0) {									//����ļ�����С�ڵ���0��ֱ�ӷ��ؼ���
			return kNotFound;
		}
		if (files_name.size() == 1 ) {									//����һ���ļ�������һ���ļ�
			if (key < files_name[0].substr(15)) {						//�Ҳ���
				return kNotFound;
			}
			else {
				//�ļ��ڶ��ֲ���
				std::ifstream input;
				input.open(dir_path_ + "/" + files_name[0]);
				input.seekg(0, std::fstream::end);
				unsigned int n = input.tellg();
				unsigned int row_length = key.size() + value_length + 2;		//��2����Ϊ���з��Ĵ���
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
						res = temp.substr(key.size());		//��ȡvalue
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
		sort(files_name.begin(), files_name.end());						//�ļ���Ŀ����һ�͵�����֮����
		//���ֶ�λҪ���ҵ��ļ�
		//end����������Ҫ��λ���ļ�
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
		//�ļ��ڶ��ֲ���
		std::ifstream input;
		input.open(dir_path_+"/"+files_name[file_end]);
		input.seekg(0, std::fstream::end);
		unsigned int n = input.tellg();
		unsigned int row_length = key.size() + value_length + 1;		//��1����Ϊ���з��Ĵ���(ʵ���Ǽ�1)
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
				res = temp.substr(key.size());		//��ȡvalue
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