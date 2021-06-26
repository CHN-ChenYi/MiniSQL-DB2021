#pragma once

#include <chrono>
#include <functional>
#include <future>
#include <map>
#include <thread>

class TaskPool {
  std::map<size_t, std::future<void>> pool_;

 public:
  /**
   * @brief Check if the task is currently running
   *
   * @param task_id the id of the task
   * @return true if the task is currently running
   */
  bool IsBusy(const size_t &task_id) {
    auto task = pool_.find(task_id);
    if (task == pool_.end()) return false;
    if (task->second.wait_for(std::chrono::milliseconds(1)) ==
        std::future_status::ready) {
      pool_.erase(task);
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
    auto task = pool_.find(task_id);
    if (task == pool_.end()) return;
    task->second.wait();
    pool_.erase(task);
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
    pool_[task_id] = std::async(std::launch::async, task);
    return true;
  }
};
