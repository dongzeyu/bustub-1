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
#include<algorithm>

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
    auto pos = find_remove_pos();
    *frame_id = circle_[pos];
    visited.erase(circle_[pos]);
    circle_.erase(circle_.begin() + pos);
    //pointer_ = (pointer_ + 1) % circle_.size(); // after every victim, pointer increase 1
    
    return true;
}

//if frame is in LRU, remove it
void ClockReplacer::Pin(frame_id_t frame_id) 
{
    std::scoped_lock lru_lock(lru_mutex);

    auto it = std::find(circle_.begin(), circle_.end(), frame_id);
    if(it != circle_.end()){
        visited.erase(*it);
        circle_.erase(it);
    }
}

//put frame into LRU
void ClockReplacer::Unpin(frame_id_t frame_id)
{
    std::scoped_lock lru_lock(lru_mutex);

    auto it = std::find(circle_.begin(), circle_.end(), frame_id);
    if(it == circle_.end()){
        if(circle_.size() == capacity){
            auto remove_pos = find_remove_pos();
            visited.erase(circle_[remove_pos]);
            circle_.erase(circle_.begin() + remove_pos);
            //pointer_ = (pointer_ + 1) % circle_.size();     // after every victim, pointer increase 1
        }
        circle_.push_back(frame_id);
        visited[frame_id] = 1;
    }
}

size_t ClockReplacer::Size()
{ 
    return circle_.size();
}

size_t ClockReplacer::find_remove_pos()
{
    // if(visited[circle_[pointer_]] == 0){
    //     return pointer_;
    // }
    auto cur_size = circle_.size();
    auto origin = pointer_;
    bool need_clear = false;
    //pointer_ = (pointer_ + 1) % cur_size;
    while(visited[circle_[pointer_]] == 1){
        pointer_ = (pointer_ + 1) % cur_size;
        if(pointer_ == origin){   //didn't find a pos with use bit 0
            need_clear = true;
            break;
        }
    }
    if(need_clear){ 
        for(auto& each : circle_){   //clear all use bit to 0
            visited[each] = 0;
        }
    }
    return pointer_;
}

}  // namespace bustub
