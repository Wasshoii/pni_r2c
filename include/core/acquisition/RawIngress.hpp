#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace openpni
{
    struct RawDataView;
}

namespace openpni::distributed::acquisition
{

    /**
     * Reserved raw-ingress slot for the worker app.
     * Real AcquisitionGrpcNode will implement this later; StubRawIngress is a no-op.
     */
    class IRawIngress
    {
    public:
        using RawDataReadyCallback = std::function<bool(const openpni::RawDataView &)>;

        virtual ~IRawIngress() = default;

        virtual void setRawDataReadyCallback(RawDataReadyCallback callback) = 0;
        virtual void setDeferRawDataRelease(bool defer) = 0;
        virtual std::function<void(uint64_t)> makeRawDataReleaseFn() = 0;
        virtual bool start() = 0;
        virtual void stop() = 0;
        virtual std::string name() const = 0;
    };

    class StubRawIngress final : public IRawIngress
    {
    public:
        void setRawDataReadyCallback(RawDataReadyCallback callback) override
        {
            m_cb = std::move(callback);
        }

        void setDeferRawDataRelease(bool) override {}

        std::function<void(uint64_t)> makeRawDataReleaseFn() override
        {
            return [](uint64_t) {};
        }

        bool start() override
        {
            m_running = true;
            return true;
        }

        void stop() override { m_running = false; }

        std::string name() const override { return "StubRawIngress"; }

        bool running() const { return m_running; }

    private:
        RawDataReadyCallback m_cb;
        bool m_running = false;
    };

    inline std::unique_ptr<IRawIngress> makeStubRawIngress()
    {
        return std::make_unique<StubRawIngress>();
    }

} // namespace openpni::distributed::acquisition
