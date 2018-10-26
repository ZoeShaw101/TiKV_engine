#include "write_ahead_log.h"
namespace polar_race {
	RetCode LogStore::Init() {				//��ʼ����ʱ���ж��Ƿ���Ҫ�����ļ����ҽ�memtable���
		if (!FileExists(log_path_)) {
			output_.open(log_path_);
			if (!output_) { return kIOError; }
			output_.close();
		}
		return kSucc;
	}
	RetCode LogStore::Read(std::map<std::string, LogItem>* memtable_ptr) {
		input_.open(log_path_);
		if (!input_) { return kIOError; }
		std::string line;
		while (getline(input_, line)) {
			LogItem cursor(line);
			(*memtable_ptr)[cursor.GetKey().ToString()] = cursor;
		}
		input_.close();
		return kSucc;
	}
	RetCode LogStore::Append(const LogItem &item) {
		pthread_mutex_lock(&mu_);
		output_.open(log_path_,std::ofstream::app);
		if (!output_) { return kIOError; }
		output_ << item.GetItem().c_str() << std::endl;
		output_.close();
		pthread_mutex_unlock(&mu_);
		return kSucc;
	}
	RetCode LogStore::Clear() {
		pthread_mutex_lock(&mu_);
		output_.open(log_path_, std::ofstream::trunc);
		if (!output_) { return kIOError; }
		output_.close();
		pthread_mutex_unlock(&mu_);
		return kSucc;
	}
	PolarString LogItem::GetKey() const{
		return PolarString(content_.substr(0, key_length_));	//keyռǰ�˸��ֽ�
	}
	PolarString LogItem::GetValue() const {
		std::string res = content_.substr(key_length_);
		PolarString temp(res);
		return temp;		//valueռ֮���4096���ֽ�
	}
	std::string LogItem::GetValueStr() const {
		std::string res = content_.substr(key_length_);
		return res;		//valueռ֮���4096���ֽ�
	}
	void LogItem::SetItem(PolarString key, PolarString value) {
		content_.clear();
		content_ = key.ToString() + value.ToString();
		key_length_ = key.size();
	}
	void LogItem::SetItem(std::string item) {
		content_ = item;
	}
	std::string LogItem::GetItem() const{
		return content_;
	}
}