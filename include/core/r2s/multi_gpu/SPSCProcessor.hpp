#pragma once

#include <atomic>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include <cstdio>
#include <pni/tools/CudaCompatibility.hpp>

namespace openpni::distributed::r2s::multi_gpu
{

template <typename Data, typename Result>
class ICompute
{
public:
    virtual ~ICompute() = default;
    virtual void compute(const Data *data, Result *out) = 0;
};

template <typename Data, typename Result>
struct DefaultResultPolicy
{
    Result make_result() const { return Result{}; }

    void prepare_for_reuse(Result &, const Data *) const noexcept {}
};

/**
 * @brief Single submit thread, single consume thread, multi-worker ordered processor.
 */
template <typename Data, typename Result, typename ResultPolicy = DefaultResultPolicy<Data, Result>>
class SPSCProcessor
{
private:
    struct Slot
    {
        std::optional<Result> result;
        std::future<void> ready;
    };

    struct ReuseState
    {
        explicit ReuseState(size_t ring_size) : slot_available(ring_size, true) {}

        std::mutex mtx;
        std::condition_variable cv;
        std::vector<bool> slot_available;
    };

public:
    using ComputePtr = std::unique_ptr<ICompute<Data, Result>>;
    using InputPtr = const Data *;
    using OutputPtr = const Result *;
    using Policy = ResultPolicy;

    enum class NextStatus
    {
        Ready,
        Failed,
        EndOfStream,
    };

    class OutputLease
    {
    public:
        OutputLease() = default;

        OutputLease(const OutputLease &) = delete;
        OutputLease &operator=(const OutputLease &) = delete;

        OutputLease(OutputLease &&other) noexcept
            : status_(other.status_),
              output_(other.output_),
              slot_index_(other.slot_index_),
              slot_keepalive_(std::move(other.slot_keepalive_)),
              reuse_state_(std::move(other.reuse_state_))
        {
            other.status_ = NextStatus::EndOfStream;
            other.output_ = nullptr;
        }

        OutputLease &operator=(OutputLease &&other) noexcept
        {
            if (this != &other)
            {
                release();
                status_ = other.status_;
                output_ = other.output_;
                slot_index_ = other.slot_index_;
                slot_keepalive_ = std::move(other.slot_keepalive_);
                reuse_state_ = std::move(other.reuse_state_);

                other.status_ = NextStatus::EndOfStream;
                other.output_ = nullptr;
            }
            return *this;
        }

        ~OutputLease() { release(); }

        explicit operator bool() const noexcept { return status_ == NextStatus::Ready && output_ != nullptr; }

        OutputPtr get() const noexcept { return output_; }
        OutputPtr operator->() const noexcept { return output_; }
        const Result &operator*() const { return *output_; }

        NextStatus status() const noexcept { return status_; }
        bool failed() const noexcept { return status_ == NextStatus::Failed; }
        bool end() const noexcept { return status_ == NextStatus::EndOfStream; }

    private:
        friend class SPSCProcessor;

        OutputLease(NextStatus status,
                    OutputPtr output,
                    size_t slot_index,
                    std::shared_ptr<Slot> slot_keepalive,
                    std::shared_ptr<ReuseState> reuse_state)
            : status_(status),
              output_(output),
              slot_index_(slot_index),
              slot_keepalive_(std::move(slot_keepalive)),
              reuse_state_(std::move(reuse_state))
        {
        }

        void release()
        {
            if (!reuse_state_)
            {
                return;
            }

            {
                std::lock_guard<std::mutex> lk(reuse_state_->mtx);
                reuse_state_->slot_available[slot_index_] = true;
            }
            reuse_state_->cv.notify_one();

            output_ = nullptr;
            slot_keepalive_.reset();
            reuse_state_.reset();
        }

        NextStatus status_{NextStatus::EndOfStream};
        OutputPtr output_{nullptr};
        size_t slot_index_{0};
        std::shared_ptr<Slot> slot_keepalive_;
        std::shared_ptr<ReuseState> reuse_state_;
    };

    static constexpr size_t DEFAULT_RING_SIZE = 16;
    static constexpr size_t DEFAULT_QUEUE_CAP = 16;

    explicit SPSCProcessor(std::vector<ComputePtr> computes)
        : SPSCProcessor(std::move(computes), DEFAULT_RING_SIZE, DEFAULT_QUEUE_CAP, Policy{})
    {
    }

    explicit SPSCProcessor(std::vector<ComputePtr> computes,
                           size_t ring_size,
                           size_t queue_cap,
                           Policy result_policy = {})
        : computes_(std::move(computes)),
          result_policy_(std::move(result_policy)),
          ring_size_(ring_size),
          queue_cap_(queue_cap),
          slots_(ring_size_),
          reuse_state_(std::make_shared<ReuseState>(ring_size_))
    {
        if (ring_size_ == 0)
        {
            throw std::invalid_argument("SPSCProcessor requires ring_size > 0");
        }

        if (queue_cap_ == 0)
        {
            throw std::invalid_argument("SPSCProcessor requires queue_cap > 0");
        }

        if (computes_.empty())
        {
            throw std::invalid_argument("SPSCProcessor requires at least one compute instance");
        }

        for (const auto &compute : computes_)
        {
            if (!compute)
            {
                throw std::invalid_argument("SPSCProcessor received a null compute instance");
            }
        }

        for (auto &slot : slots_)
        {
            slot = std::make_shared<Slot>();
            slot->result.emplace(result_policy_.make_result());
        }

        for (size_t i = 0; i < computes_.size(); ++i)
        {
            workers_.emplace_back(&SPSCProcessor::worker_loop, this, i);
        }
    }

    ~SPSCProcessor() { wait_idle(); }

    SPSCProcessor(const SPSCProcessor &) = delete;
    SPSCProcessor &operator=(const SPSCProcessor &) = delete;
    SPSCProcessor(SPSCProcessor &&) = delete;
    SPSCProcessor &operator=(SPSCProcessor &&) = delete;

    size_t ring_size() const noexcept { return ring_size_; }
    size_t queue_cap() const noexcept { return queue_cap_; }

    void submit(InputPtr data)
    {
        int seq = seq_.fetch_add(1, std::memory_order_relaxed);
        const size_t slot_index = static_cast<size_t>(seq) % ring_size_;
        acquire_slot(slot_index);

        if (stop_.load(std::memory_order_acquire))
        {
            release_slot(slot_index);
            return;
        }

        auto slot = slots_[slot_index];
        std::promise<void> prom;
        slot->ready = prom.get_future();

        last_submitted_.store(seq, std::memory_order_release);

        {
            std::lock_guard<std::mutex> lk(cv_slot_mtx_);
            slot_filled_count_.store(seq + 1, std::memory_order_release);
        }
        cv_slot_.notify_all();

        if (!push_task({seq, data, std::move(slot), std::move(prom)}))
        {
            release_slot(slot_index);
        }
    }

    OutputLease next()
    {
        if (!wait_for_next_slot())
        {
            return {};
        }

        const int seq = next_to_pop_.fetch_add(1, std::memory_order_acq_rel);
        const size_t slot_index = static_cast<size_t>(seq) % ring_size_;
        auto slot = slots_[slot_index];

        try
        {
            slot->ready.get();
            OutputPtr output = &slot->result.value();
            return OutputLease(NextStatus::Ready, output, slot_index, std::move(slot), reuse_state_);
        }
        catch (const std::exception &e)
        {
            // 不留下原始信息的话，上层只能看到一句 "compute failed"，无从定位。
            // 这个头文件被不链接 glog 的目标包含，因此直接写 stderr。
            std::fprintf(stderr, "[SPSCProcessor] compute slot %zu failed: %s\n",
                         slot_index, e.what());
            release_slot(slot_index);
            return OutputLease(NextStatus::Failed, nullptr, 0, nullptr, nullptr);
        }
        catch (...)
        {
            std::fprintf(stderr,
                         "[SPSCProcessor] compute slot %zu failed with unknown exception\n",
                         slot_index);
            release_slot(slot_index);
            return OutputLease(NextStatus::Failed, nullptr, 0, nullptr, nullptr);
        }
    }

    void signal_no_more_data()
    {
        no_more_data_.store(true, std::memory_order_release);
        cv_slot_.notify_all();
    }

    int pending_count() const
    {
        return last_submitted_.load(std::memory_order_acquire) - next_to_pop_.load(std::memory_order_acquire) + 1;
    }

private:
    struct Task
    {
        int seq;
        InputPtr data;
        std::shared_ptr<Slot> slot;
        std::promise<void> prom;
    };

    bool wait_for_next_slot()
    {
        std::unique_lock<std::mutex> lk(cv_slot_mtx_);
        cv_slot_.wait(lk, [this] {
            const int next_to_pop = next_to_pop_.load(std::memory_order_acquire);
            return stop_.load(std::memory_order_acquire) ||
                   next_to_pop < static_cast<int>(slot_filled_count_.load(std::memory_order_acquire)) ||
                   (no_more_data_.load(std::memory_order_acquire) &&
                    next_to_pop > last_submitted_.load(std::memory_order_acquire));
        });

        if (stop_.load(std::memory_order_acquire))
        {
            return false;
        }

        const int next_to_pop = next_to_pop_.load(std::memory_order_acquire);
        if (no_more_data_.load(std::memory_order_acquire) &&
            next_to_pop > last_submitted_.load(std::memory_order_acquire))
        {
            return false;
        }

        return true;
    }

    void acquire_slot(size_t slot_index)
    {
        std::unique_lock<std::mutex> lk(reuse_state_->mtx);
        reuse_state_->cv.wait(lk, [this, slot_index] {
            return stop_.load(std::memory_order_acquire) || reuse_state_->slot_available[slot_index];
        });

        if (!stop_.load(std::memory_order_acquire))
        {
            reuse_state_->slot_available[slot_index] = false;
        }
    }

    void release_slot(size_t slot_index)
    {
        {
            std::lock_guard<std::mutex> lk(reuse_state_->mtx);
            reuse_state_->slot_available[slot_index] = true;
        }
        reuse_state_->cv.notify_one();
    }

    void worker_loop(size_t compute_index)
    {
        auto *compute = computes_[compute_index].get();

        while (true)
        {
            Task task;
            if (!pop_task(task))
            {
                break;
            }

            try
            {
                auto &result = task.slot->result.value();
                prepare_result_for_reuse(result, task.data);
                compute->compute(task.data, &result);
                task.prom.set_value();
            }
            catch (...)
            {
                task.prom.set_exception(std::current_exception());
            }
        }
    }

    bool push_task(Task t)
    {
        std::unique_lock<std::mutex> lk(mtx_);
        cv_not_full_.wait(lk, [this] { return task_queue_.size() < queue_cap_ || stop_; });
        if (stop_)
        {
            return false;
        }
        task_queue_.push(std::move(t));
        cv_not_empty_.notify_one();
        return true;
    }

    bool pop_task(Task &t)
    {
        std::unique_lock<std::mutex> lk(mtx_);
        cv_not_empty_.wait(lk, [this] { return !task_queue_.empty() || stop_; });
        if (stop_ && task_queue_.empty())
        {
            return false;
        }
        t = std::move(task_queue_.front());
        task_queue_.pop();
        cv_not_full_.notify_one();
        cv_empty_.notify_one();
        return true;
    }

    void wait_idle()
    {
        {
            std::unique_lock<std::mutex> lk(mtx_);
            cv_empty_.wait(lk, [this] { return task_queue_.empty(); });
        }
        stop_.store(true, std::memory_order_release);
        cv_not_empty_.notify_all();
        cv_not_full_.notify_all();
        cv_slot_.notify_all();
        reuse_state_->cv.notify_all();
        for (auto &worker : workers_)
        {
            if (worker.joinable())
            {
                worker.join();
            }
        }
    }

    void prepare_result_for_reuse(Result &result, InputPtr data)
    {
        if constexpr (requires(Policy &policy, Result &result_ref, InputPtr data_ptr) {
                          policy.prepare_for_reuse(result_ref, data_ptr);
                      })
        {
            result_policy_.prepare_for_reuse(result, data);
        }
    }

    std::vector<ComputePtr> computes_;
    std::vector<std::thread> workers_;
    Policy result_policy_;
    const size_t ring_size_;
    const size_t queue_cap_;

    std::atomic<int> seq_{0};
    std::atomic<int> last_submitted_{-1};
    std::atomic<bool> no_more_data_{false};
    std::atomic<bool> stop_{false};
    std::atomic<int> next_to_pop_{0};
    std::atomic<int> slot_filled_count_{0};

    std::mutex cv_slot_mtx_;
    std::condition_variable cv_slot_;

    std::vector<std::shared_ptr<Slot>> slots_;
    std::shared_ptr<ReuseState> reuse_state_;

    std::queue<Task> task_queue_;
    std::mutex mtx_;
    std::condition_variable cv_not_full_;
    std::condition_variable cv_not_empty_;
    std::condition_variable cv_empty_;
};

} // namespace openpni::distributed::r2s::multi_gpu
