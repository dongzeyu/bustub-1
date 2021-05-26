//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) : capacity(num_pages), pointer_(0)
{

}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id)
{ 
    std::scoped_lock lru_lock(lru_mutex);

    if(circle_.empty()) {
        return false;
    }
    if(circle_[pointer_].second == 0){
        *frame_id = circle_[pointer_].first;
        circle_.erase(circle_.begin() + pointer_);
        return true;
    }
    
    return true; 
}

//if frame is in LRU, remove it
void ClockReplacer::Pin(frame_id_t frame_id) {}

void ClockReplacer::Unpin(frame_id_t frame_id) {}

size_t ClockReplacer::Size() { return 0; }

}  // namespace bustub
