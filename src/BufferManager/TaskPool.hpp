#pragma once

#include <chrono>
#include <functional>
#include <future>
#include <map>
#include <thread>

#define UseThreadPool
#ifdef UseThreadPool
#include "ThreadPool.hpp"
#endif

class TaskPool {
#ifdef UseThreadPool
  ThreadPool *thread_pool_;
#endif
  std::map<size_t, std::future<void>> task_pool_;

 public:
#ifdef UseThreadPool
  /**
   * @brief Construct a new Task Pool object
   *
   */
  TaskPool() {
    thread_pool_ = new ThreadPool(std::thread::hardware_concurrency() - 1);
  }

  /**
   * @brief Destroy the Task Pool object
   *
   */
  ~TaskPool() { delete thread_pool_; }
#endif

  /**
   * @brief Check if the task is currently running
   *
   * @param task_id the id of the task
   * @return true if the task is currently running
   */
  bool IsBusy(const size_t &task_id) {
    auto task = task_pool_.find(task_id);
    if (task == task_pool_.end()) return false;
    if (task->second.wait_for(std::chrono::milliseconds(1)) ==
        std::future_status::ready) {
      task_pool_.erase(task);
      return false;
    }
    return true;
  }

  /**
   * @brief Wait until the task is completed
   *
   * @param task_id the id of the task
   */
  void Wait(const size_t &task_id) {
    auto task = task_pool_.find(task_id);
    if (task == task_pool_.end()) return;
    task->second.wait();
    task_pool_.erase(task);
  }

  /**
   * @brief Bind the task to an id and execute it using a new thread
   *
   * @param task_id the id of the task
   * @param task the task
   * @return true if the id is being using
   */
  bool Exec(const size_t &task_id, std::function<void(void)> task) {
    if (IsBusy(task_id)) {
      return false;
    }
#ifndef UseThreadPool
    task_pool_[task_id] = std::async(std::launch::async, task);
#else
    task_pool_[task_id] = thread_pool_->push([task](int) { task(); });
#endif
    return true;
  }
};
