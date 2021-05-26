//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.h
//
// Identification: src/include/buffer/clock_replacer.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <vector>

#include <deque>
#include <unordered_map>
#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {


// struct Entry{
//   frame_id_t frame_id;
//   int visited;
//   bool dirty;
//   Entry(frame_id_t id, int v, bool d=false) : frame_id(id), visited(v), dirty(d)
//   {

//   }
// };

/**
 * ClockReplacer implements the clock replacement policy, which approximates the Least Recently Used policy.
 */
class ClockReplacer : public Replacer {
 public:
  /**
   * Create a new ClockReplacer.
   * @param num_pages the maximum number of pages the ClockReplacer will be required to store
   */
  explicit ClockReplacer(size_t num_pages);

  /**
   * Destroys the ClockReplacer.
   */
  ~ClockReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  std::deque<frame_id_t> circle_;
  std::unordered_map<frame_id_t, int> visited;
  size_t capacity;
  size_t pointer_;
  std::mutex lru_mutex;

  size_t find_remove_pos();
};

}  // namespace bustub
