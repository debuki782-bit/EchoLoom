// Experimental 4.1.10.27 native CDN adapter. See README before loading.
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <bcrypt.h>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include "MinHook.h"

#pragma comment(lib, "bcrypt.lib")

namespace {
constexpr uintptr_t kStart = 0x21ab5e0;
constexpr uintptr_t kConstruct = 0x196410;
constexpr uintptr_t kDestruct = 0x199630;
constexpr uintptr_t kGrowString = 0x54470;
constexpr uintptr_t kCallbackVtable = 0x865bc38;
constexpr size_t kRequestBytes = 0x588;
constexpr char kSha[] = "f487e4f56930456a25fb0d7b058c77e2dcd364e268d49bdfff4dc0e3be26ba36";
using Start = int(__fastcall*)(void*);
uintptr_t base = 0;
Start originalStart = nullptr;
std::mutex callbackMutex;
std::shared_ptr<void> callback;
std::atomic<unsigned long> observed{0};
std::atomic<unsigned long> submitted{0};
std::atomic<int> lastResult{0};
struct Resource {
    std::string aeskey;
    ULONGLONG captured;
    unsigned type;
    uint64_t bytes;
};
std::unordered_map<std::string, Resource> resources;
std::string lastFileid;
static_assert(sizeof(std::shared_ptr<void>) == 16, "MSVC x64 shared_ptr ABI required");

bool CheckFingerprint(HMODULE module) {
    wchar_t path[32768]{};
    if (!GetModuleFileNameW(module, path, 32768)) return false;
    std::ifstream input(path, std::ios::binary);
    if (!input) return false;
    BCRYPT_ALG_HANDLE algorithm = nullptr;
    BCRYPT_HASH_HANDLE hash = nullptr;
    DWORD objectSize = 0, written = 0;
    bool ok = BCryptOpenAlgorithmProvider(&algorithm, BCRYPT_SHA256_ALGORITHM, nullptr, 0) == 0;
    if (ok) ok = BCryptGetProperty(algorithm, BCRYPT_OBJECT_LENGTH,
        reinterpret_cast<PUCHAR>(&objectSize), sizeof(objectSize), &written, 0) == 0;
    std::vector<unsigned char> object(objectSize);
    if (ok) ok = BCryptCreateHash(algorithm, &hash, object.data(), objectSize, nullptr, 0, 0) == 0;
    std::array<char, 65536> buffer{};
    while (ok && input) {
        input.read(buffer.data(), buffer.size());
        const auto size = input.gcount();
        if (size) ok = BCryptHashData(hash, reinterpret_cast<PUCHAR>(buffer.data()), static_cast<ULONG>(size), 0) == 0;
    }
    if (!input.eof()) ok = false;
    unsigned char digest[32]{};
    if (ok) ok = BCryptFinishHash(hash, digest, sizeof(digest), 0) == 0;
    if (hash) BCryptDestroyHash(hash);
    if (algorithm) BCryptCloseAlgorithmProvider(algorithm, 0);
    char hex[65]{};
    for (size_t i = 0; i < 32; ++i) sprintf_s(hex + i * 2, 3, "%02x", digest[i]);
    return ok && std::strcmp(hex, kSha) == 0;
}

int __fastcall ObserveStart(void* request) {
    // The observed native caller supplies a shared_ptr<ICdnCallback> at +0.
    // Reuse the actual account-scoped adapter; never invent a callback vtable.
    // A new task key has no associated UI task, so file completion is verified
    // independently by the caller. The shared_ptr retains native ownership.
    const auto pair = static_cast<void**>(request);
    if (pair[0] && pair[1] && *static_cast<uintptr_t*>(pair[0]) == base + kCallbackVtable) {
        std::lock_guard<std::mutex> lock(callbackMutex);
        callback = *reinterpret_cast<const std::shared_ptr<void>*>(request);
        const auto bytes = static_cast<unsigned char*>(request);
        const auto& fileid = *reinterpret_cast<const std::string*>(bytes + 0x70);
        const auto& key = *reinterpret_cast<const std::string*>(bytes + 0x90);
        if (!fileid.empty() && fileid.size() <= 4096 && key.size() == 32) {
            if (resources.size() >= 256) resources.clear();
            resources[fileid] = {key, GetTickCount64(), *reinterpret_cast<unsigned*>(bytes + 0xd0),
                *reinterpret_cast<uint64_t*>(bytes + 0x198)};
            lastFileid = fileid;
        }
        ++observed;
    }
    return originalStart(request);
}

// These functions use only primitive arguments so SEH does not skip C++
// unwinding. A native exception is reported and disables further submissions;
// catching one is not proof that the host process remains healthy.
bool Construct(void* request) {
    __try {
        reinterpret_cast<void(__fastcall*)(void*)>(base + kConstruct)(request);
        return true;
    } __except (EXCEPTION_EXECUTE_HANDLER) { return false; }
}
bool Destruct(void* request) {
    __try {
        reinterpret_cast<void(__fastcall*)(void*)>(base + kDestruct)(request);
        return true;
    } __except (EXCEPTION_EXECUTE_HANDLER) { return false; }
}
bool Assign(void* field, const char* text, size_t size) {
    __try {
        auto words = static_cast<uintptr_t*>(field);
        if (size > words[3]) {
            reinterpret_cast<void*(__fastcall*)(void*, size_t, void*, const char*)>(base + kGrowString)
                (field, size, nullptr, text);
        } else {
            char* destination = words[3] < 16 ? static_cast<char*>(field) : reinterpret_cast<char*>(words[0]);
            std::memcpy(destination, text, size);
            destination[size] = '\0';
            words[2] = size;
        }
        return true;
    } __except (EXCEPTION_EXECUTE_HANDLER) { return false; }
}
int CallStart(void* request) {
    __try { return originalStart(request); }
    __except (EXCEPTION_EXECUTE_HANDLER) { return INT_MIN; }
}
}

extern "C" __declspec(dllexport) int ImageDownloadInitialize() {
    if (originalStart) return 0;
    const auto module = GetModuleHandleW(L"Weixin.dll");
    if (!module || !CheckFingerprint(module)) return -1;
    base = reinterpret_cast<uintptr_t>(module);
    const unsigned char prefix[]{0x55, 0x56, 0x57, 0x48, 0x81};
    if (std::memcmp(reinterpret_cast<void*>(base + kStart), prefix, sizeof(prefix))) return -2;
    auto status = MH_Initialize();
    if (status != MH_OK && status != MH_ERROR_ALREADY_INITIALIZED) return -3;
    status = MH_CreateHook(reinterpret_cast<void*>(base + kStart), ObserveStart, reinterpret_cast<void**>(&originalStart));
    if (status != MH_OK) { originalStart = nullptr; return -4; }
    status = MH_EnableHook(reinterpret_cast<void*>(base + kStart));
    if (status != MH_OK) {
        MH_RemoveHook(reinterpret_cast<void*>(base + kStart));
        originalStart = nullptr;
        return -5;
    }
    return 0;
}

extern "C" __declspec(dllexport) int ImageDownloadReady() {
    std::lock_guard<std::mutex> lock(callbackMutex);
    return callback && originalStart ? 1 : 0;
}

extern "C" __declspec(dllexport) int ImageDownloadSubmit(
    const char* fileid, const char* aeskey, const char* destination,
    const char* taskKey, unsigned type, uint64_t expectedBytes) {
    if (!fileid || !aeskey || !destination || !taskKey ||
        !*fileid || std::strlen(fileid) > 4096 || std::strlen(aeskey) != 32 ||
        std::strlen(destination) > 32760 || std::strlen(taskKey) < 16 ||
        std::strlen(taskKey) > 128 || (type != 1 && type != 2) || expectedBytes > 200000000)
        return -100;
    std::shared_ptr<void> retained;
    {
        std::lock_guard<std::mutex> lock(callbackMutex);
        retained = callback;
    }
    if (!originalStart || !retained || lastResult == INT_MIN) return -101;
    alignas(16) unsigned char request[kRequestBytes]{};
    if (!Construct(request)) { lastResult = INT_MIN; return INT_MIN; }
    // The constructor initializes native containers and default fields. Every
    // string allocation and free stays in Weixin.dll's own allocator.
    *reinterpret_cast<std::shared_ptr<void>*>(request) = retained;
    bool ok = Assign(request + 0x50, taskKey, std::strlen(taskKey)) &&
        Assign(request + 0x70, fileid, std::strlen(fileid)) &&
        Assign(request + 0x90, aeskey, std::strlen(aeskey)) &&
        Assign(request + 0xb0, destination, std::strlen(destination));
    *reinterpret_cast<unsigned*>(request + 0xd0) = type;
    *reinterpret_cast<uint64_t*>(request + 0x198) = expectedBytes;
    int result = ok ? CallStart(request) : INT_MIN;
    if (ok) ++submitted;
    if (!Destruct(request)) result = INT_MIN;
    lastResult = result;
    return result;
}

extern "C" __declspec(dllexport) int ImageDownloadSubmitKnown(
    const char* fileid, const char* destination, const char* taskKey,
    unsigned type, uint64_t expectedBytes) {
    if (!fileid) return -100;
    std::string key;
    {
        std::lock_guard<std::mutex> lock(callbackMutex);
        const auto found = resources.find(fileid);
        if (found == resources.end() || GetTickCount64() - found->second.captured > 30 * 60 * 1000)
            return -102;
        key = found->second.aeskey;
    }
    return ImageDownloadSubmit(fileid, key.c_str(), destination, taskKey, type, expectedBytes);
}

#ifdef IMAGE_DOWNLOAD_PROBE
extern "C" __declspec(dllexport) int ImageDownloadProbe(const char* destination) {
    std::string id;
    Resource sample;
    {
        std::lock_guard<std::mutex> lock(callbackMutex);
        if (lastFileid.empty()) return -102;
        id = lastFileid;
        sample = resources.at(id);
    }
    if (sample.type != 1 && sample.type != 2) return -103;
    const auto key = "codex_native_probe_" + std::to_string(GetTickCount64());
    return ImageDownloadSubmit(id.c_str(), sample.aeskey.c_str(), destination, key.c_str(), sample.type, sample.bytes);
}
#endif

BOOL WINAPI DllMain(HINSTANCE instance, DWORD reason, LPVOID) {
    if (reason == DLL_PROCESS_ATTACH) DisableThreadLibraryCalls(instance);
    // Initialization and hook changes happen explicitly outside the loader lock.
    return TRUE;
}
