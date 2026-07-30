#include "dataplane/rdma/HugepageArena.hpp"

#include <cerrno>
#include <cstring>
#include <utility>

#include <sys/mman.h>
#include <unistd.h>

#include <iostream>
#define LOG(severity) ::std::cerr
#define VLOG(level) if(true) ; else ::std::cerr
#define LOG_EVERY_N(severity, n) ::std::cerr

namespace openpni::distributed::dataplane::rdma
{

namespace
{
size_t roundUp(size_t value, size_t align)
{
    if (align == 0)
    {
        return value;
    }
    return (value + align - 1) / align * align;
}
} // namespace

HugepageArena::~HugepageArena()
{
    release();
}

HugepageArena::HugepageArena(HugepageArena &&other) noexcept
{
    *this = std::move(other);
}

HugepageArena &HugepageArena::operator=(HugepageArena &&other) noexcept
{
    if (this != &other)
    {
        release();
        m_data = other.m_data;
        m_size = other.m_size;
        m_huge = other.m_huge;
        other.m_data = nullptr;
        other.m_size = 0;
        other.m_huge = false;
    }
    return *this;
}

bool HugepageArena::allocate(size_t bytes, bool preferHugePages)
{
    release();
    if (bytes == 0)
    {
        return false;
    }

    const size_t page = static_cast<size_t>(::sysconf(_SC_PAGESIZE));
    const size_t mapped = roundUp(bytes, page);

    if (preferHugePages)
    {
#ifdef MAP_HUGETLB
        void *ptr = ::mmap(nullptr, mapped, PROT_READ | PROT_WRITE,
                           MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
        if (ptr != MAP_FAILED)
        {
            m_data = ptr;
            m_size = mapped;
            m_huge = true;
            std::memset(m_data, 0, m_size);
            return true;
        }
        VLOG(1) << "HugepageArena MAP_HUGETLB failed (" << std::strerror(errno)
                << "); falling back to anonymous mmap";
#endif
    }

    void *ptr = ::mmap(nullptr, mapped, PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (ptr == MAP_FAILED)
    {
        LOG(ERROR) << "HugepageArena mmap failed: " << std::strerror(errno);
        return false;
    }

    m_data = ptr;
    m_size = mapped;
    m_huge = false;
    std::memset(m_data, 0, m_size);

    // Best-effort advise; ignore failure.
#ifdef MADV_HUGEPAGE
    ::madvise(m_data, m_size, MADV_HUGEPAGE);
#endif
    return true;
}

void HugepageArena::release()
{
    if (m_data && m_size > 0)
    {
        ::munmap(m_data, m_size);
    }
    m_data = nullptr;
    m_size = 0;
    m_huge = false;
}

} // namespace openpni::distributed::dataplane::rdma
