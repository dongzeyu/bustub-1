//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <algorithm>

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : capacity(num_pages)
{

}

LRUReplacer::~LRUReplacer() = default;



bool LRUReplacer::Victim(frame_id_t *frame_id) 
{ 
    std::scoped_lock lru_lock(lru_mutex);
    
    if(queue_.empty()){
        return false;
    }
    *frame_id = queue_.back();
    queue_.pop_back();
    return true; 
}

//means some thread is using  the frame, so it should not be in LRU
void LRUReplacer::Pin(frame_id_t frame_id) 
{
    std::scoped_lock lru_lock(lru_mutex);
    auto it = find(queue_.begin(), queue_.end(), frame_id);
    if( it!= queue_.end()){
        queue_.erase(it);
    }
}

//means the frame is not be used, it can put in LRU.
//suppose the insert sequence 1,2,3,1
//before insert the second 1, the LRU is 3->2->1, 
//when insert another 1, the position of 1 shoule be adjusted., because it's just visited.
void LRUReplacer::Unpin(frame_id_t frame_id) 
{
    std::scoped_lock lru_lock(lru_mutex);
    auto it = find(queue_.begin(), queue_.end(), frame_id);
    if(it == queue_.end()){
        if(queue_.size() == capacity){
            queue_.pop_back();
        }
        queue_.push_front(frame_id);
    }
}

size_t LRUReplacer::Size() 
{ 
    return queue_.size(); 
}

}  // namespace bustub
