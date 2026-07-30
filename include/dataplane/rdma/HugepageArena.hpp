#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace openpni::distributed::dataplane::rdma
{

/**
 * Contiguous memory arena preferred on hugepages; falls back to anonymous mmap.
 * Suitable for ibv_reg_mr registration.
 */
class HugepageArena
{
public:
    HugepageArena() = default;
    ~HugepageArena();

    HugepageArena(const HugepageArena &) = delete;
    HugepageArena &operator=(const HugepageArena &) = delete;
    HugepageArena(HugepageArena &&other) noexcept;
    HugepageArena &operator=(HugepageArena &&other) noexcept;

    /** Allocate `bytes` (rounded up to page size). Returns false on failure. */
    bool allocate(size_t bytes, bool preferHugePages = true);

    void release();

    void *data() noexcept { return m_data; }
    const void *data() const noexcept { return m_data; }
    size_t size() const noexcept { return m_size; }
    bool usesHugePages() const noexcept { return m_huge; }
    explicit operator bool() const noexcept { return m_data != nullptr && m_size > 0; }

private:
    void *m_data = nullptr;
    size_t m_size = 0;
    bool m_huge = false;
};

} // namespace openpni::distributed::dataplane::rdma
