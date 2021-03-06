// Copyright [2018] Alibaba Cloud All rights reserved
#include "engine_race.h"

#include <iostream>
#include <algorithm>

namespace polar_race {

RetCode Engine::Open(const std::string& name, Engine** eptr) {
  return EngineRace::Open(name, eptr);
}

Engine::~Engine() {
}

/*
 * Complete the functions below to implement you own engine
 */

// 1. Open engine
RetCode EngineRace::Open(const std::string& name, Engine** eptr) {
  *eptr = NULL;
  EngineRace *engine_race = new EngineRace(name);

  *eptr = engine_race;
  return kSucc;
}

// 2. Close engine
EngineRace::~EngineRace() {
}

// 3. Write a key-value pair into engine
RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
  store.add(key.ToString(), value.ToString());
  return kSucc;
}

// 4. Read value of a key
RetCode EngineRace::Read(const PolarString& key, std::string* value) {
  auto it = store.find(key.ToString());
  if (it == store.end()) return kNotFound;
  *value = it->second;
  return kSucc;
}

/*
 * NOTICE: Implement 'Range' in quarter-final,
 *         you can skip it in preliminary.
 */
// 5. Applies the given Vistor::Visit function to the result
// of every key-value pair in the key range [first, last),
// in order
// lower=="" is treated as a key before all keys in the database.
// upper=="" is treated as a key after all keys in the database.
// Therefore the following call will traverse the entire database:
//   Range("", "", visitor)
RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
    Visitor &visitor) {
  threadsafe_map<std::string, std::string>::const_iterator lowerIt, upperIt;
  
  if (lower == "") {
    lowerIt = store.begin();
  } else {
    lowerIt = store.lower_bound(lower.ToString());
  }
  
  if (upper == "") {
    upperIt = store.end();
  } else {
    upperIt = store.lower_bound(upper.ToString());
  }
  
  while (lowerIt != upperIt) {
    visitor.Visit(lowerIt->first, lowerIt->second);
    ++lowerIt;
  }
  
  return kSucc;
}

}  // namespace polar_race
