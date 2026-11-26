// Experimental 4.1.10.27 native CDN adapter. See README before loading.
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <bcrypt.h>
#include <array>
#include <atomic>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "MinHook.h"

#pragma comment(lib, "bcrypt.lib")

namespace {
constexpr uintptr_t kStart = 0x21ab5e0;
constexpr uintptr_t kStartUpload = 0x21ab410;
constexpr uintptr_t kUploadCompleted = 0x21b1df0;
constexpr uintptr_t kConstruct = 0x196410;
constexpr uintptr_t kDestruct = 0x199630;
constexpr uintptr_t kDestructUpload = 0x98b250;
constexpr uintptr_t kGrowString = 0x54470;
constexpr uintptr_t kCallbackVtable = 0x865bc38;
constexpr size_t kRequestBytes = 0x588;
constexpr size_t kUploadRequestBytes = 0x3f0;
constexpr uint64_t kMaxTransferBytes = 200000000ULL;
constexpr char kSha[] = "f487e4f56930456a25fb0d7b058c77e2dcd364e268d49bdfff4dc0e3be26ba36";
using Start = int(__fastcall*)(void*);
using UploadCompletedFn = int(__fastcall*)(void*, const void*, const void*);
uintptr_t base = 0;
Start originalStart = nullptr;
Start originalStartUpload = nullptr;
UploadCompletedFn originalUploadCompleted = nullptr;
std::mutex callbackMutex;
std::shared_ptr<void> callback;
std::atomic<unsigned long> observed{0};
std::atomic<unsigned long> submitted{0};
std::atomic<int> lastResult{0};
std::atomic<int> lastUploadResult{0};
struct Resource {
    std::string aeskey;
    ULONGLONG captured;
    unsigned type;
    uint64_t bytes;
};
struct UploadCompletion {
    int code;
    std::string fileid;
    std::string aeskey;
    uint64_t bytes;
};
std::unordered_map<std::string, Resource> resources;
std::unordered_set<std::string> trackedUploads;
std::unordered_map<std::string, UploadCompletion> completedUploads;
std::string lastFileid;
static_assert(sizeof(std::shared_ptr<void>) == 16, "MSVC x64 shared_ptr ABI required");

bool IsAsciiHex(const char* text, size_t len) {
    if (!text || !len) return false;
    for (size_t i = 0; i < len; ++i) {
        if (!std::isxdigit(static_cast<unsigned char>(text[i]))) return false;
    }
    return true;
}

bool TryReadWxStringRaw(const void* field, char* outBuf, size_t outCap, size_t* outLen) {
    __try {
        if (!field || !outBuf || !outLen || outCap == 0) return false;
        const auto words = static_cast<const uintptr_t*>(field);
        const size_t len = words[2];
        const size_t cap = words[3];
        if (len >= outCap || cap < len) return false;
        const char* src = cap < 16 ? static_cast<const char*>(field) : reinterpret_cast<const char*>(words[0]);
        if (!src) return false;
        std::memcpy(outBuf, src, len);
        outBuf[len] = '\0';
        *outLen = len;
        return true;
    } __except (EXCEPTION_EXECUTE_HANDLER) {
        return false;
    }
}

std::string ReadWxStringBounded(const void* field, size_t maxLen) {
    std::vector<char> buf(maxLen + 1, '\0');
    size_t outLen = 0;
    if (!TryReadWxStringRaw(field, buf.data(), buf.size(), &outLen)) return {};
    return std::string(buf.data(), outLen);
}

void CaptureCallback(void* request) {
    const auto pair = static_cast<void**>(request);
    if (pair[0] && pair[1] && *static_cast<uintptr_t*>(pair[0]) == base + kCallbackVtable) {
        std::lock_guard<std::mutex> lock(callbackMutex);
        callback = *reinterpret_cast<const std::shared_ptr<void>*>(request);
    }
}

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

int __fastcall ObserveUploadStart(void* request) {
    CaptureCallback(request);
    return originalStartUpload(request);
}

bool TryReadUploadResultScalars(const void* resultPtr, int* outCode, uint64_t* outBytes) {
    __try {
        if (!resultPtr || !outCode || !outBytes) return false;
        const auto bytes = static_cast<const unsigned char*>(resultPtr);
        *outCode = *reinterpret_cast<const int32_t*>(bytes + 0x10);
        *outBytes = *reinterpret_cast<const uint64_t*>(bytes + 0x150);
        return true;
    } __except (EXCEPTION_EXECUTE_HANDLER) {
        return false;
    }
}

int __fastcall ObserveUploadCompleted(void* self, const void* taskKeyPtr, const void* resultPtr) {
    const std::string key = ReadWxStringBounded(taskKeyPtr, 256);
    if (!key.empty()) {
        bool isTracked = false;
        {
            std::lock_guard<std::mutex> lock(callbackMutex);
            isTracked = trackedUploads.find(key) != trackedUploads.end();
        }
        if (isTracked) {
            int code = -1;
            uint64_t byteCount = 0;
            std::string fileid;
            std::string aeskey;
            if (TryReadUploadResultScalars(resultPtr, &code, &byteCount) && code == 0) {
                const auto resBytes = static_cast<const unsigned char*>(resultPtr);
                // C2CUploadResult: +0x70 = fileid (hex >= 60), +0x90 = aeskey (hex == 32);
                // check primary slots first then fallback string slots safely.
                constexpr size_t kCandidateOffsets[] = {0x70, 0x90, 0x48, 0xb0, 0xd0, 0xf0, 0x130};
                for (size_t off : kCandidateOffsets) {
                    const std::string candidate = ReadWxStringBounded(resBytes + off, 4096);
                    if (candidate.empty()) continue;
                    if (aeskey.empty() && candidate.size() == 32 && IsAsciiHex(candidate.data(), candidate.size())) {
                        aeskey = candidate;
                    } else if (fileid.empty() && candidate.size() >= 60 && IsAsciiHex(candidate.data(), candidate.size())) {
                        fileid = candidate;
                    }
                }
            }
            std::lock_guard<std::mutex> lock(callbackMutex);
            if (completedUploads.size() >= 256) completedUploads.clear();
            completedUploads[key] = UploadCompletion{code, fileid, aeskey, byteCount};
            trackedUploads.erase(key);
        }
    }
    return originalUploadCompleted(self, taskKeyPtr, resultPtr);
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
bool DestructUpload(void* request) {
    __try {
        reinterpret_cast<void(__fastcall*)(void*)>(base + kDestructUpload)(request);
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
int CallStartUpload(void* request) {
    __try { return originalStartUpload(request); }
    __except (EXCEPTION_EXECUTE_HANDLER) { return INT_MIN; }
}

void InitUploadRequestFields(unsigned char* req, unsigned fileType) {
    std::memset(req, 0, kUploadRequestBytes);
    constexpr size_t kStringOffsets[] = {
        0x10, 0x30, 0x58, 0x80, 0xa0, 0xd0, 0xf0, 0x120,
        0x140, 0x160, 0x180, 0x1a0, 0x200, 0x228, 0x258, 0x278,
        0x298, 0x2b8, 0x2d8, 0x300, 0x320, 0x348, 0x368, 0x390,
        0x3b0, 0x3d0,
    };
    for (size_t off : kStringOffsets) {
        auto words = reinterpret_cast<uintptr_t*>(req + off);
        words[2] = 0;
        words[3] = 0xF;
    }
    *reinterpret_cast<uint32_t*>(req + 0x50) = 1;
    *reinterpret_cast<uint32_t*>(req + 0x54) = 1;
    req[0xc0] = 1;
    *reinterpret_cast<uint32_t*>(req + 0xc4) = fileType;
    *reinterpret_cast<uint64_t*>(req + 0x118) = UINT64_MAX;
    *reinterpret_cast<uint32_t*>(req + 0x1fc) = 1;
    *reinterpret_cast<uint64_t*>(req + 0x248) = 1;
    req[0x251] = 1;
    req[0x2f9] = 1;
    *reinterpret_cast<uint32_t*>(req + 0x388) = 0x50;
}

int SubmitDownloadInternal(
    const char* fileid, const char* aeskey, const char* destination,
    const char* taskKey, unsigned type, uint64_t expectedBytes) {
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
}

extern "C" __declspec(dllexport) int ImageDownloadInitialize() {
    if (originalStart) return 0;
    const auto module = GetModuleHandleW(L"Weixin.dll");
    if (!module || !CheckFingerprint(module)) return -1;
    base = reinterpret_cast<uintptr_t>(module);
    const unsigned char startPrefix[]{0x55, 0x56, 0x57, 0x48, 0x81};
    const unsigned char uploadCbPrefix[]{0x55, 0x41, 0x57, 0x41, 0x56};
    const unsigned char uploadDtorPrefix[]{0x41, 0x56, 0x56, 0x57, 0x53};
    if (std::memcmp(reinterpret_cast<void*>(base + kStart), startPrefix, sizeof(startPrefix)) ||
        std::memcmp(reinterpret_cast<void*>(base + kStartUpload), startPrefix, sizeof(startPrefix)) ||
        std::memcmp(reinterpret_cast<void*>(base + kUploadCompleted), uploadCbPrefix, sizeof(uploadCbPrefix)) ||
        std::memcmp(reinterpret_cast<void*>(base + kDestructUpload), uploadDtorPrefix, sizeof(uploadDtorPrefix))) {
        return -2;
    }
    auto status = MH_Initialize();
    if (status != MH_OK && status != MH_ERROR_ALREADY_INITIALIZED) return -3;

    auto install = [](uintptr_t rva, LPVOID detour, LPVOID* original) -> bool {
        if (MH_CreateHook(reinterpret_cast<void*>(base + rva), detour, original) != MH_OK) return false;
        if (MH_EnableHook(reinterpret_cast<void*>(base + rva)) != MH_OK) {
            MH_RemoveHook(reinterpret_cast<void*>(base + rva));
            *original = nullptr;
            return false;
        }
        return true;
    };

    if (!install(kStart, ObserveStart, reinterpret_cast<LPVOID*>(&originalStart))) {
        originalStart = nullptr;
        return -4;
    }
    if (!install(kStartUpload, ObserveUploadStart, reinterpret_cast<LPVOID*>(&originalStartUpload))) {
        MH_RemoveHook(reinterpret_cast<void*>(base + kStart));
        originalStart = nullptr;
        originalStartUpload = nullptr;
        return -5;
    }
    if (!install(kUploadCompleted, ObserveUploadCompleted, reinterpret_cast<LPVOID*>(&originalUploadCompleted))) {
        MH_RemoveHook(reinterpret_cast<void*>(base + kStartUpload));
        MH_RemoveHook(reinterpret_cast<void*>(base + kStart));
        originalStart = nullptr;
        originalStartUpload = nullptr;
        originalUploadCompleted = nullptr;
        return -6;
    }
    return 0;
}

extern "C" __declspec(dllexport) int ImageDownloadReady() {
    std::lock_guard<std::mutex> lock(callbackMutex);
    return callback && originalStart ? 1 : 0;
}

extern "C" __declspec(dllexport) int FileUploadReady() {
    std::lock_guard<std::mutex> lock(callbackMutex);
    return callback && originalStartUpload && originalUploadCompleted && lastUploadResult != INT_MIN ? 1 : 0;
}

extern "C" __declspec(dllexport) int ImageDownloadSubmit(
    const char* fileid, const char* aeskey, const char* destination,
    const char* taskKey, unsigned type, uint64_t expectedBytes) {
    if (!fileid || !aeskey || !destination || !taskKey ||
        !*fileid || std::strlen(fileid) > 4096 || std::strlen(aeskey) != 32 ||
        !IsAsciiHex(aeskey, 32) || std::strlen(destination) > 32760 ||
        std::strlen(taskKey) < 16 || std::strlen(taskKey) > 128 ||
        (type != 1 && type != 2) || expectedBytes > kMaxTransferBytes)
        return -100;
    return SubmitDownloadInternal(fileid, aeskey, destination, taskKey, type, expectedBytes);
}

extern "C" __declspec(dllexport) int FileDownloadSubmit(
    const char* fileid, const char* aeskey, const char* destination,
    const char* taskKey, unsigned type, uint64_t expectedBytes) {
    if (!fileid || !aeskey || !destination || !taskKey ||
        !*fileid || std::strlen(fileid) > 4096 || std::strlen(aeskey) != 32 ||
        !IsAsciiHex(aeskey, 32) || !*destination || std::strlen(destination) > 32760 ||
        std::strlen(taskKey) < 16 || std::strlen(taskKey) > 128 ||
        (type != 5 && type != 7) || expectedBytes > kMaxTransferBytes)
        return -100;
    return SubmitDownloadInternal(fileid, aeskey, destination, taskKey, type, expectedBytes);
}

extern "C" __declspec(dllexport) int FileUploadSubmit(
    const char* filepath, const char* taskKey, unsigned fileType) {
    if (!filepath || !taskKey || !*filepath || std::strlen(filepath) > 32760 ||
        std::strlen(taskKey) < 16 || std::strlen(taskKey) > 128 ||
        (fileType != 5 && fileType != 7)) {
        return -100;
    }
    std::shared_ptr<void> retained;
    {
        std::lock_guard<std::mutex> lock(callbackMutex);
        retained = callback;
        if (!originalStartUpload || !originalUploadCompleted || !retained || lastUploadResult == INT_MIN) {
            return -101;
        }
        if (trackedUploads.size() >= 256) trackedUploads.clear();
        trackedUploads.insert(taskKey);
        completedUploads.erase(taskKey);
    }

    alignas(16) unsigned char request[kUploadRequestBytes]{};
    InitUploadRequestFields(request, fileType);
    *reinterpret_cast<std::shared_ptr<void>*>(request) = retained;
    const size_t keyLen = std::strlen(taskKey);
    const size_t pathLen = std::strlen(filepath);
    const bool ok = Assign(request + 0x10, taskKey, keyLen) &&
        Assign(request + 0x58, taskKey, keyLen) &&
        Assign(request + 0x120, filepath, pathLen);
    int result = ok ? CallStartUpload(request) : INT_MIN;
    if (!DestructUpload(request)) result = INT_MIN;
    if (result != 0) {
        std::lock_guard<std::mutex> lock(callbackMutex);
        trackedUploads.erase(taskKey);
    }
    lastUploadResult = result;
    return result;
}

extern "C" __declspec(dllexport) int FileUploadPopResult(
    const char* taskKey, int* outCode, char* outFileId, size_t fileIdCap,
    char* outAesKey, size_t aesKeyCap, uint64_t* outBytes) {
    if (!taskKey || !*taskKey || !outCode || !outFileId || fileIdCap == 0 ||
        !outAesKey || aesKeyCap == 0 || !outBytes) {
        return -100;
    }
    UploadCompletion item;
    {
        std::lock_guard<std::mutex> lock(callbackMutex);
        const auto found = completedUploads.find(taskKey);
        if (found == completedUploads.end()) return 0;
        item = found->second;
        completedUploads.erase(found);
    }
    if (item.fileid.size() >= fileIdCap || item.aeskey.size() >= aesKeyCap) {
        return -104;
    }
    *outCode = item.code;
    *outBytes = item.bytes;
    if (!item.fileid.empty()) std::memcpy(outFileId, item.fileid.data(), item.fileid.size());
    outFileId[item.fileid.size()] = '\0';
    if (!item.aeskey.empty()) std::memcpy(outAesKey, item.aeskey.data(), item.aeskey.size());
    outAesKey[item.aeskey.size()] = '\0';
    return 1;
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
