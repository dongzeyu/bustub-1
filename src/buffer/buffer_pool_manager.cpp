//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size); // LRUcache has the same size as buffer pool

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) 
{
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  std::scoped_lock bpm_lock(latch_);


  //case1: the asked page is in LRU cache
  auto it = page_table_.find(page_id);
  if(it != page_table_.end()){
    auto frame_id = it->second;
    auto frame = pages_ + frame_id;
    // if(frame->GetPinCount() == 0){
    //   frame->pin_count_ = 1;
    // }
    frame->pin_count_++;
    replacer_->Pin(frame_id);
    
    return frame;
  }

  if(!is_all_pinned()){
    auto frame_id = find_replace_frame();
    auto frame = pages_ + frame_id;
    page_table_[page_id] = frame_id;
    init_new_page(page_id, frame_id);
    disk_manager_->ReadPage(page_id, frame->GetData());
    return frame;
  }
  
  return nullptr;
}
 
 //when a thread is not using one page anymore, the page shoule be unpinned.
 //is_dirty: false for read and true for write.
 //decrease the pin_count and put it into LRU
bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty)
{ 
  std::scoped_lock bpm_lock(latch_);

  auto it = page_table_.find(page_id);
  if(it == page_table_.end()){ //not in buffer pool
    return false;
  }
  
  auto frame_id = it->second;
  auto frame = pages_ + frame_id;
  if(frame->pin_count_ > 0){
    frame->pin_count_--;
  }
  if(frame->pin_count_ == 0){
    replacer_->Unpin(frame_id);  //put into LRU
  }
  
  frame->is_dirty_ |= is_dirty;

  return true;
}

//write the page back to disk
bool BufferPoolManager::FlushPageImpl(page_id_t page_id) 
{
  // Make sure you call DiskManager::WritePage!

  std::scoped_lock bpm_lock(latch_);

  auto it = page_table_.find(page_id);
  if(it == page_table_.end()){  //the page is not in disk
    return false;
  }
  auto frame = pages_ + it->second;
  disk_manager_->WritePage(page_id, frame->GetData());
  frame->is_dirty_ = false;   //when flush to disk, the frame's dirty bit should reset to false
  return true;
}

//need to set the page_id parameter to new_page_id 
Page *BufferPoolManager::NewPageImpl(page_id_t *page_id)
{
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  std::scoped_lock bpm_lock(latch_);

  //all pinned
  if(is_all_pinned()){
    return nullptr;
  }

  auto new_page_id = disk_manager_->AllocatePage();

  auto frame_id = find_replace_frame();
  init_new_page(new_page_id, frame_id);
  pages_[frame_id].ResetMemory();
  page_table_[new_page_id] = frame_id;
  *page_id = new_page_id;
  return &pages_[frame_id];

}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id)
{
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  std::scoped_lock bpm_lock(latch_);

  disk_manager_->DeallocatePage(page_id);

  auto it = page_table_.find(page_id);
  if(it == page_table_.end()){      //If P does not exist, return true.
    return true;
  }
  auto frame_id = it->second;
  auto frame = pages_ + frame_id;
  if(frame->GetPinCount() > 0){
    return false;
  }
  // if(frame->IsDirty()){
  //   FlushPageImpl(page_id);
  // }

  replacer_->Pin(frame_id); //remove from LRU, because it should be in free_list

  page_table_.erase(page_id);
  //frame->ResetMemory();
  frame->page_id_ = INVALID_PAGE_ID;
  //frame->pin_count_ = 0;
  free_list_.emplace_back(static_cast<int>(frame_id));  //return it to free_list
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() 
{
  // You can do it!

}

//find a free frame in free_list or LRUcache.
frame_id_t BufferPoolManager::find_replace_frame()
{
  frame_id_t relpace_id = INVALID_PAGE_ID;
  if(!free_list_.empty()){
    relpace_id = free_list_.front();
    free_list_.pop_front();
  }
  else{
    if(replacer_->Size() > 0){
      replacer_->Victim(&relpace_id);
      if(pages_[relpace_id].IsDirty()){
        //FlushPageImpl(pages_[relpace_id].page_id_);
        disk_manager_->WritePage(pages_[relpace_id].page_id_, pages_[relpace_id].GetData());
      }
      page_table_.erase(pages_[relpace_id].page_id_);
    }
  }
  return relpace_id;
}


}  // namespace bustub
