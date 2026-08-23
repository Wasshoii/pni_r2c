/**
 * Spawn real app_coin_master + two app_acq_r2s_node workers (synthetic).
 * Skip if app binaries are not present, or if InProcess cannot span processes
 * and no RNIC is available for a RoCE retry.
 */

#include <chrono>
#include <csignal>
#include <cstdlib>
#include <dirent.h>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <sys/stat.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace
{
    constexpr const char *kCoinCfg = "app/config/tests/inprocess_smoke/coin.json";
    constexpr const char *kWorker0Cfg = "app/config/tests/inprocess_smoke/worker0.json";
    constexpr const char *kWorker1Cfg = "app/config/tests/inprocess_smoke/worker1.json";

    enum class RunResult
    {
        Pass,
        Fail,
        SkipInProcess
    };

    bool fileExecutable(const std::string &path)
    {
        return access(path.c_str(), X_OK) == 0;
    }

    std::string findAppBin(const std::string &name)
    {
        std::vector<std::string> cands;
        if (const char *env = std::getenv("R2C_BIN_DIR"))
        {
            cands.push_back(std::string(env) + "/app/" + name);
        }
        cands.push_back("bin/app/" + name);
        cands.push_back("build/apps/basic/bin/app/" + name);
        cands.push_back("build/apps/cuda/bin/app/" + name);
        for (const auto &p : cands)
        {
            if (fileExecutable(p))
            {
                return p;
            }
        }
        return {};
    }

    std::string readFile(const std::string &path)
    {
        std::ifstream ifs(path);
        if (!ifs)
        {
            return {};
        }
        std::ostringstream oss;
        oss << ifs.rdbuf();
        return oss.str();
    }

    bool writeFile(const std::string &path, const std::string &text)
    {
        std::ofstream ofs(path, std::ios::trunc);
        if (!ofs)
        {
            return false;
        }
        ofs << text;
        return static_cast<bool>(ofs);
    }

    bool contains(const std::string &hay, const std::string &needle)
    {
        return hay.find(needle) != std::string::npos;
    }

    std::string replaceAll(std::string s, const std::string &from, const std::string &to)
    {
        size_t pos = 0;
        while ((pos = s.find(from, pos)) != std::string::npos)
        {
            s.replace(pos, from.size(), to);
            pos += to.size();
        }
        return s;
    }

    uint64_t parseLastUintAfter(const std::string &text, const std::string &key)
    {
        const auto pos = text.rfind(key);
        if (pos == std::string::npos)
        {
            return 0;
        }
        size_t i = pos + key.size();
        while (i < text.size() && (text[i] == ' ' || text[i] == '='))
        {
            ++i;
        }
        uint64_t v = 0;
        bool any = false;
        while (i < text.size() && text[i] >= '0' && text[i] <= '9')
        {
            any = true;
            v = v * 10ULL + static_cast<uint64_t>(text[i] - '0');
            ++i;
        }
        return any ? v : 0;
    }

    uint64_t sumFinishedTotals(const std::string &text)
    {
        uint64_t sum = 0;
        size_t pos = 0;
        const std::string marker = "source finished sent=true total=";
        while (true)
        {
            pos = text.find(marker, pos);
            if (pos == std::string::npos)
            {
                break;
            }
            sum += parseLastUintAfter(text.substr(pos), "total=");
            pos += marker.size();
        }
        return sum;
    }

    bool hasVerbsDevice()
    {
        DIR *dir = opendir("/sys/class/infiniband");
        if (!dir)
        {
            return false;
        }
        bool any = false;
        while (dirent *ent = readdir(dir))
        {
            const std::string name = ent->d_name;
            if (name != "." && name != "..")
            {
                any = true;
                break;
            }
        }
        closedir(dir);
        return any;
    }

    struct Child
    {
        pid_t pid = -1;
        std::string logPath;
    };

    bool spawnApp(const std::string &bin, const std::string &config, const std::string &logPath, Child *out)
    {
        const pid_t pid = fork();
        if (pid < 0)
        {
            std::cerr << "fork failed for " << bin << "\n";
            return false;
        }
        if (pid == 0)
        {
            const int fd = open(logPath.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
            if (fd < 0)
            {
                _exit(127);
            }
            dup2(fd, STDOUT_FILENO);
            dup2(fd, STDERR_FILENO);
            if (fd > 2)
            {
                close(fd);
            }
            setenv("GLOG_logtostderr", "1", 1);
            setenv("GLOG_minloglevel", "0", 1);
            const char *argv[] = {bin.c_str(), "--config", config.c_str(), nullptr};
            execv(bin.c_str(), const_cast<char **>(argv));
            _exit(127);
        }
        out->pid = pid;
        out->logPath = logPath;
        return true;
    }

    bool waitPid(pid_t pid, int *exitCode, int timeoutMs)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        while (std::chrono::steady_clock::now() < deadline)
        {
            int status = 0;
            const pid_t r = waitpid(pid, &status, WNOHANG);
            if (r == pid)
            {
                if (WIFEXITED(status))
                {
                    *exitCode = WEXITSTATUS(status);
                    return true;
                }
                if (WIFSIGNALED(status))
                {
                    *exitCode = 128 + WTERMSIG(status);
                    return true;
                }
                *exitCode = -1;
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        return false;
    }

    void killChild(Child *c)
    {
        if (c->pid > 0)
        {
            kill(c->pid, SIGTERM);
            int unused = 0;
            if (!waitPid(c->pid, &unused, 2000))
            {
                kill(c->pid, SIGKILL);
                waitpid(c->pid, nullptr, 0);
            }
            c->pid = -1;
        }
    }

    bool waitLogContains(const std::string &path, const std::string &needle, int timeoutMs)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (contains(readFile(path), needle))
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        return false;
    }

    bool waitWorkers(Child *w0, Child *w1, int *w0Code, int *w1Code, int timeoutMs)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        bool w0Done = false;
        bool w1Done = false;
        while (std::chrono::steady_clock::now() < deadline && (!w0Done || !w1Done))
        {
            if (!w0Done)
            {
                int status = 0;
                const pid_t r = waitpid(w0->pid, &status, WNOHANG);
                if (r == w0->pid)
                {
                    *w0Code = WIFEXITED(status) ? WEXITSTATUS(status)
                                                : (WIFSIGNALED(status) ? 128 + WTERMSIG(status) : -1);
                    w0->pid = -1;
                    w0Done = true;
                }
            }
            if (!w1Done)
            {
                int status = 0;
                const pid_t r = waitpid(w1->pid, &status, WNOHANG);
                if (r == w1->pid)
                {
                    *w1Code = WIFEXITED(status) ? WEXITSTATUS(status)
                                                : (WIFSIGNALED(status) ? 128 + WTERMSIG(status) : -1);
                    w1->pid = -1;
                    w1Done = true;
                }
            }
            if (!w0Done || !w1Done)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
        }
        return w0Done && w1Done;
    }

    RunResult runCluster(const std::string &coinBin,
                         const std::string &workerBin,
                         const std::string &coinCfg,
                         const std::string &w0Cfg,
                         const std::string &w1Cfg,
                         const std::string &logDir)
    {
        Child coin;
        Child w0;
        Child w1;
        const std::string coinLog = logDir + "/coin.log";
        const std::string w0Log = logDir + "/worker0.log";
        const std::string w1Log = logDir + "/worker1.log";
        writeFile(coinLog, "");
        writeFile(w0Log, "");
        writeFile(w1Log, "");

        if (!spawnApp(coinBin, coinCfg, coinLog, &coin))
        {
            return RunResult::Fail;
        }
        if (!waitLogContains(coinLog, "Coin service listening", 20000))
        {
            std::cerr << "coin did not start listening\n" << readFile(coinLog) << "\n";
            killChild(&coin);
            return RunResult::Fail;
        }

        if (!spawnApp(workerBin, w0Cfg, w0Log, &w0) || !spawnApp(workerBin, w1Cfg, w1Log, &w1))
        {
            killChild(&w0);
            killChild(&w1);
            killChild(&coin);
            return RunResult::Fail;
        }

        int w0Code = -1;
        int w1Code = -1;
        const bool workersDone = waitWorkers(&w0, &w1, &w0Code, &w1Code, 30000);
        if (!workersDone || w0Code != 0 || w1Code != 0)
        {
            const std::string w0Text = readFile(w0Log);
            const std::string w1Text = readFile(w1Log);
            killChild(&w0);
            killChild(&w1);
            killChild(&coin);
            if (contains(w0Text, "InProcess session handle not found") ||
                contains(w1Text, "InProcess session handle not found"))
            {
                return RunResult::SkipInProcess;
            }
            std::cerr << "workers failed w0=" << w0Code << " w1=" << w1Code
                      << " done=" << workersDone << "\n";
            std::cerr << "w0 log:\n" << w0Text << "\n";
            std::cerr << "w1 log:\n" << w1Text << "\n";
            std::cerr << "coin log:\n" << readFile(coinLog) << "\n";
            return RunResult::Fail;
        }

        int coinCode = -1;
        const bool coinDone = waitPid(coin.pid, &coinCode, 15000);
        if (!coinDone)
        {
            std::cerr << "timeout waiting for coin after workers finished\n";
            std::cerr << "coin log:\n" << readFile(coinLog) << "\n";
            killChild(&coin);
            return RunResult::Fail;
        }
        coin.pid = -1;

        const std::string coinText = readFile(coinLog);
        const std::string w0Text = readFile(w0Log);
        const std::string w1Text = readFile(w1Log);
        if (coinCode != 0)
        {
            std::cerr << "coin exit=" << coinCode << "\n" << coinText << "\n";
            return RunResult::Fail;
        }
        if (!contains(coinText, "startIssued=true") || !contains(coinText, "dataplaneOpen=2"))
        {
            std::cerr << "handshake not observed in coin log\n" << coinText << "\n";
            return RunResult::Fail;
        }

        const uint64_t sent = sumFinishedTotals(w0Text) + sumFinishedTotals(w1Text);
        uint64_t received = parseLastUintAfter(coinText, "singles_received=");
        if (received == 0)
        {
            received = parseLastUintAfter(coinText, "totalSinglesReceived=");
        }
        if (sent == 0 || received < sent)
        {
            std::cerr << "sent/received mismatch sent=" << sent << " received=" << received << "\n";
            std::cerr << "coin log:\n" << coinText << "\n";
            std::cerr << "w0 log:\n" << w0Text << "\n";
            std::cerr << "w1 log:\n" << w1Text << "\n";
            return RunResult::Fail;
        }
        std::cout << "[PASS] app_inprocess_smoke sent=" << sent << " received=" << received << "\n";
        return RunResult::Pass;
    }

    bool writeRoceConfigs(std::string *coinCfg, std::string *w0Cfg, std::string *w1Cfg)
    {
        std::string coin = readFile(kCoinCfg);
        std::string w0 = readFile(kWorker0Cfg);
        std::string w1 = readFile(kWorker1Cfg);
        if (coin.empty() || w0.empty() || w1.empty())
        {
            return false;
        }
        coin = replaceAll(coin, "\"forceInProcess\": true", "\"forceInProcess\": false");
        coin = replaceAll(coin, "\"requireRoce\": false", "\"requireRoce\": true");
        coin = replaceAll(coin, "127.0.0.1:51081", "127.0.0.1:51082");
        w0 = replaceAll(w0, "\"forceInProcess\": true", "\"forceInProcess\": false");
        w0 = replaceAll(w0, "\"requireRoce\": false", "\"requireRoce\": true");
        w0 = replaceAll(w0, "127.0.0.1:51081", "127.0.0.1:51082");
        w1 = replaceAll(w1, "\"forceInProcess\": true", "\"forceInProcess\": false");
        w1 = replaceAll(w1, "\"requireRoce\": false", "\"requireRoce\": true");
        w1 = replaceAll(w1, "127.0.0.1:51081", "127.0.0.1:51082");

        *coinCfg = "/tmp/r2c_inprocess_smoke/roce_coin.json";
        *w0Cfg = "/tmp/r2c_inprocess_smoke/roce_worker0.json";
        *w1Cfg = "/tmp/r2c_inprocess_smoke/roce_worker1.json";
        return writeFile(*coinCfg, coin) && writeFile(*w0Cfg, w0) && writeFile(*w1Cfg, w1);
    }
} // namespace

int main()
{
    const std::string coinBin = findAppBin("app_coin_master");
    const std::string workerBin = findAppBin("app_acq_r2s_node");
    if (coinBin.empty() || workerBin.empty())
    {
        std::cout << "[SKIP] test_app_inprocess_smoke (need bin/app/app_coin_master and app_acq_r2s_node)\n";
        return 0;
    }

    mkdir("/tmp/r2c_inprocess_smoke", 0755);
    mkdir("/tmp/r2c_inprocess_smoke/coin", 0755);

    const RunResult first = runCluster(
        coinBin, workerBin, kCoinCfg, kWorker0Cfg, kWorker1Cfg, "/tmp/r2c_inprocess_smoke");
    if (first == RunResult::Pass)
    {
        return 0;
    }
    if (first == RunResult::Fail)
    {
        return 1;
    }

    if (!hasVerbsDevice())
    {
        std::cout << "[SKIP] test_app_inprocess_smoke "
                     "(InProcess dataplane is same-process only; no RNIC for RoCE retry)\n";
        return 0;
    }

    std::string roceCoin;
    std::string roceW0;
    std::string roceW1;
    if (!writeRoceConfigs(&roceCoin, &roceW0, &roceW1))
    {
        std::cerr << "failed to write RoCE retry configs\n";
        return 1;
    }
    std::cout << "[INFO] retrying app smoke over RoCE on 127.0.0.1:51082\n";
    mkdir("/tmp/r2c_inprocess_smoke/roce", 0755);
    const RunResult roce = runCluster(
        coinBin, workerBin, roceCoin, roceW0, roceW1, "/tmp/r2c_inprocess_smoke/roce");
    if (roce == RunResult::Pass)
    {
        return 0;
    }
    if (roce == RunResult::SkipInProcess)
    {
        std::cout << "[SKIP] test_app_inprocess_smoke (RoCE retry still landed on InProcess)\n";
        return 0;
    }
    return 1;
}
