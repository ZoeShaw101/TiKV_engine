// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef INCLUDE_ENGINE_H_
#define INCLUDE_ENGINE_H_
#include <string>
#include "include/polar_string.h"

namespace polar_race {

enum RetCode {
  kSucc = 0,			//成功
  kNotFound = 1,		//没有找到
  kCorruption = 2,		//物理坏块
  kNotSupported = 3,	//不支持
  kInvalidArgument = 4,	//无效的参数
  kIOError = 5,			//IO错误
  kIncomplete = 6,		//数据库残缺
  kTimedOut = 7,		//超时
  kFull = 8,			//满
  kOutOfMemory = 9,		//内存溢出
};

// Pass to Engine::Range for callback
class Visitor {
 public:
  virtual ~Visitor() {}

  virtual void Visit(const PolarString &key, const PolarString &value) = 0;
};
//数据库引擎
class Engine {
 public:
  // Open engine
  static RetCode Open(const std::string& name,
      Engine** eptr);

  Engine() { }

  // Close engine
  virtual ~Engine();

  // Write a key-value pair into engine
  virtual RetCode Write(const PolarString& key,
      const PolarString& value) = 0;

  // Read value of a key
  virtual RetCode Read(const PolarString& key,
      std::string* value) = 0;


  /*
   * NOTICE: Implement 'Range' in quarter-final,
   *         you can skip it in preliminary.
   */
  // Applies the given Vistor::Visit function to the result
  // of every key-value pair in the key range [first, last),
  // in order
  // lower=="" is treated as a key before all keys in the database.
  // upper=="" is treated as a key after all keys in the database.
  // Therefore the following call will traverse the entire database:
  //   Range("", "", visitor)
  virtual RetCode Range(const PolarString& lower,
      const PolarString& upper,
      Visitor &visitor) = 0;
};

}  // namespace polar_race

#endif  // INCLUDE_ENGINE_H_
