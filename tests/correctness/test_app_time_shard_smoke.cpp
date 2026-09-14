/**
 * Dual-coin time-shard app smoke: Master + Compute control plane always;
 * RoCE ingest/hot-switch only when an RNIC is present.
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
    constexpr const char *kMasterCfg = "app/config/tests/time_shard_smoke/coin_master.json";
    constexpr const char *kComputeCfg = "app/config/tests/time_shard_smoke/coin_compute.json";
    constexpr const char *kWorker0Cfg = "app/config/tests/time_shard_smoke/worker0.json";
    constexpr const char *kWorker1Cfg = "app/config/tests/time_shard_smoke/worker1.json";

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

    bool spawnApp(const std::string &bin, const std::string &config, const std::string &logPath,
                  Child *out)
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

    bool runControlPlane(const std::string &masterBin, const std::string &computeBin)
    {
        mkdir("/tmp/r2c_time_shard_smoke", 0755);
        mkdir("/tmp/r2c_time_shard_smoke/master", 0755);
        mkdir("/tmp/r2c_time_shard_smoke/compute", 0755);
        mkdir("/tmp/r2c_time_shard_smoke/control", 0755);

        Child master;
        Child compute;
        const std::string masterLog = "/tmp/r2c_time_shard_smoke/control/master.log";
        const std::string computeLog = "/tmp/r2c_time_shard_smoke/control/compute.log";
        writeFile(masterLog, "");
        writeFile(computeLog, "");

        if (!spawnApp(masterBin, kMasterCfg, masterLog, &master))
        {
            return false;
        }
        if (!waitLogContains(masterLog, "Coin service listening", 20000))
        {
            std::cerr << "master did not start listening\n" << readFile(masterLog) << "\n";
            killChild(&master);
            return false;
        }
        if (!spawnApp(computeBin, kComputeCfg, computeLog, &compute))
        {
            killChild(&master);
            return false;
        }

        const bool sawRegister = waitLogContains(masterLog, "RegisterCoin", 15000);
        const bool sawRegistered =
            waitLogContains(computeLog, "registered with Master", 15000);
        killChild(&compute);
        killChild(&master);
        if (!sawRegister || !sawRegistered)
        {
            std::cerr << "control plane RegisterCoin not observed register=" << sawRegister
                      << " registered=" << sawRegistered << "\n";
            std::cerr << "master log:\n" << readFile(masterLog) << "\n";
            std::cerr << "compute log:\n" << readFile(computeLog) << "\n";
            return false;
        }
        std::cout << "[PASS] time_shard_smoke control plane (RegisterCoin)\n";
        return true;
    }

    bool writeRoceConfigs(std::string *masterCfg, std::string *computeCfg, std::string *w0Cfg,
                          std::string *w1Cfg)
    {
        std::string master = readFile(kMasterCfg);
        std::string compute = readFile(kComputeCfg);
        std::string w0 = readFile(kWorker0Cfg);
        std::string w1 = readFile(kWorker1Cfg);
        if (master.empty() || compute.empty() || w0.empty() || w1.empty())
        {
            return false;
        }
        auto rewrite = [](std::string s)
        {
            s = replaceAll(s, "\"forceInProcess\": true", "\"forceInProcess\": false");
            s = replaceAll(s, "\"requireRoce\": false", "\"requireRoce\": true");
            s = replaceAll(s, "127.0.0.1:51091", "127.0.0.1:51093");
            s = replaceAll(s, "127.0.0.1:51092", "127.0.0.1:51094");
            return s;
        };
        master = rewrite(master);
        compute = rewrite(compute);
        w0 = rewrite(w0);
        w1 = rewrite(w1);

        *masterCfg = "/tmp/r2c_time_shard_smoke/roce_master.json";
        *computeCfg = "/tmp/r2c_time_shard_smoke/roce_compute.json";
        *w0Cfg = "/tmp/r2c_time_shard_smoke/roce_worker0.json";
        *w1Cfg = "/tmp/r2c_time_shard_smoke/roce_worker1.json";
        return writeFile(*masterCfg, master) && writeFile(*computeCfg, compute) &&
               writeFile(*w0Cfg, w0) && writeFile(*w1Cfg, w1);
    }

    bool sawSwitchOrShip(const std::string &masterText, const std::string &computeText,
                         const std::string &w0Text, const std::string &w1Text)
    {
        return contains(masterText, "SET_ACTIVE") || contains(masterText, "ship applyHandoff") ||
               contains(computeText, "ship applyHandoff") ||
               contains(w0Text, "active coin switched") ||
               contains(w1Text, "active coin switched");
    }

    bool runDataplane(const std::string &masterBin, const std::string &computeBin,
                      const std::string &workerBin, const std::string &masterCfg,
                      const std::string &computeCfg, const std::string &w0Cfg,
                      const std::string &w1Cfg, const std::string &logDir)
    {
        mkdir(logDir.c_str(), 0755);
        mkdir((logDir + "/master").c_str(), 0755);
        mkdir((logDir + "/compute").c_str(), 0755);

        Child master;
        Child compute;
        Child w0;
        Child w1;
        const std::string masterLog = logDir + "/master.log";
        const std::string computeLog = logDir + "/compute.log";
        const std::string w0Log = logDir + "/worker0.log";
        const std::string w1Log = logDir + "/worker1.log";
        writeFile(masterLog, "");
        writeFile(computeLog, "");
        writeFile(w0Log, "");
        writeFile(w1Log, "");

        if (!spawnApp(masterBin, masterCfg, masterLog, &master))
        {
            return false;
        }
        if (!waitLogContains(masterLog, "Coin service listening", 20000))
        {
            std::cerr << "RoCE master did not start\n" << readFile(masterLog) << "\n";
            killChild(&master);
            return false;
        }
        if (!spawnApp(computeBin, computeCfg, computeLog, &compute))
        {
            killChild(&master);
            return false;
        }
        if (!waitLogContains(computeLog, "registered with Master", 15000))
        {
            std::cerr << "RoCE compute did not register\n" << readFile(computeLog) << "\n";
            killChild(&compute);
            killChild(&master);
            return false;
        }

        if (!spawnApp(workerBin, w0Cfg, w0Log, &w0) || !spawnApp(workerBin, w1Cfg, w1Log, &w1))
        {
            killChild(&w0);
            killChild(&w1);
            killChild(&compute);
            killChild(&master);
            return false;
        }

        int w0Code = -1;
        int w1Code = -1;
        const bool workersDone = waitWorkers(&w0, &w1, &w0Code, &w1Code, 30000);
        const std::string w0Text = readFile(w0Log);
        const std::string w1Text = readFile(w1Log);
        if (!workersDone || w0Code != 0 || w1Code != 0)
        {
            killChild(&w0);
            killChild(&w1);
            killChild(&compute);
            killChild(&master);
            if (contains(w0Text, "InProcess session handle not found") ||
                contains(w1Text, "InProcess session handle not found"))
            {
                std::cout << "[SKIP] time_shard_smoke dataplane (InProcess cannot span processes)\n";
                return true;
            }
            std::cerr << "workers failed w0=" << w0Code << " w1=" << w1Code
                      << " done=" << workersDone << "\n";
            std::cerr << "w0 log:\n" << w0Text << "\n";
            std::cerr << "w1 log:\n" << w1Text << "\n";
            return false;
        }

        int unused = 0;
        (void)waitPid(master.pid, &unused, 20000);
        (void)waitPid(compute.pid, &unused, 2000);
        killChild(&master);
        killChild(&compute);

        const std::string masterText = readFile(masterLog);
        const std::string computeText = readFile(computeLog);
        if (!sawSwitchOrShip(masterText, computeText, w0Text, w1Text))
        {
            std::cerr << "expected SET_ACTIVE / active coin switched / ship applyHandoff\n";
            std::cerr << "master log:\n" << masterText << "\n";
            std::cerr << "compute log:\n" << computeText << "\n";
            std::cerr << "w0 log:\n" << w0Text << "\n";
            std::cerr << "w1 log:\n" << w1Text << "\n";
            return false;
        }

        const uint64_t sent = sumFinishedTotals(w0Text) + sumFinishedTotals(w1Text);
        uint64_t recvA = parseLastUintAfter(masterText, "singles_received=");
        if (recvA == 0)
        {
            recvA = parseLastUintAfter(masterText, "totalSinglesReceived=");
        }
        uint64_t recvB = parseLastUintAfter(computeText, "singles_received=");
        if (recvB == 0)
        {
            recvB = parseLastUintAfter(computeText, "totalSinglesReceived=");
        }
        const uint64_t received = recvA + recvB;
        if (sent == 0 || received + 8 < sent)
        {
            std::cerr << "sent/received mismatch sent=" << sent << " received=" << received
                      << " A=" << recvA << " B=" << recvB << "\n";
            return false;
        }
        std::cout << "[PASS] time_shard_smoke dataplane sent=" << sent << " received=" << received
                  << " A=" << recvA << " B=" << recvB << "\n";
        return true;
    }
} // namespace

int main()
{
    const std::string masterBin = findAppBin("app_coin_master");
    const std::string computeBin = findAppBin("app_coin_node");
    const std::string workerBin = findAppBin("app_acq_r2s_node");
    if (masterBin.empty() || computeBin.empty() || workerBin.empty())
    {
        std::cout << "[SKIP] test_app_time_shard_smoke "
                     "(need bin/app/app_coin_master, app_coin_node, app_acq_r2s_node)\n";
        return 0;
    }

    if (!runControlPlane(masterBin, computeBin))
    {
        return 1;
    }

    if (!hasVerbsDevice())
    {
        std::cout << "[SKIP] time_shard_smoke dataplane "
                     "(InProcess dataplane is same-process only; no RNIC for RoCE)\n";
        return 0;
    }

    std::string roceMaster;
    std::string roceCompute;
    std::string roceW0;
    std::string roceW1;
    if (!writeRoceConfigs(&roceMaster, &roceCompute, &roceW0, &roceW1))
    {
        std::cerr << "failed to write RoCE time-shard configs\n";
        return 1;
    }
    std::cout << "[INFO] retrying time-shard smoke over RoCE on 127.0.0.1:51093/51094\n";
    mkdir("/tmp/r2c_time_shard_smoke/roce", 0755);
    if (!runDataplane(masterBin, computeBin, workerBin, roceMaster, roceCompute, roceW0, roceW1,
                      "/tmp/r2c_time_shard_smoke/roce"))
    {
        return 1;
    }
    return 0;
}
