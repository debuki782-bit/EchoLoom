# WeChat PC 4.1.10.27 Hook 一键构建与接入指南（Windows x64）

> 本目录只包含**最小补丁、独立原图下载扩展源码与一键构建脚本**，不包含任何上游 Hook 源码或微信安装包。构建脚本会在本地直接拉取上游开源仓库的固定提交并编译。

## 1. 环境与对应微信版本

- **微信桌面端版本**：WeChat (Weixin) Windows x64 **`4.1.10.27`**（请自行安装对应版本并关闭自动更新，仓库不分发微信安装包）
  - 对应 `Weixin.dll` SHA-256：`F487E4F56930456A25FB0D7B058C77E2DCD364E268D49BDFFF4DC0E3BE26BA36`
- **基础 Hook 上游仓库**：[`mosheng20205/WechatHook4.1.10.27`](https://github.com/mosheng20205/WechatHook4.1.10.27)
  - 锁定提交：`713ec7d6f01d858e067a623c9bf2cda64d0d3e19`（归档 SHA-256：`6580544B5020CE41A2D7479B2EB0DD76057B8E2DC7FA4B9F87FCB3BB1CEB4189`）
- **编译环境**：Windows 10/11 x64 + Git + Python 3.12+ + Visual Studio 2022 Build Tools（`C++ 桌面开发 / v143` 工具集）：
  ```powershell
  winget install Microsoft.VisualStudio.2022.BuildTools --override "--wait --quiet --add Microsoft.VisualStudio.Workload.VCTools --includeRecommended"
  ```

## 2. 我们对上游 Hook 改了什么

补丁 [`patches/0001-echoloom-hardening-and-atlist.patch`](patches/0001-echoloom-hardening-and-atlist.patch) 仅对上游固定提交做了 4 处收敛改动：

1. **回环安全加固**：将 HTTP 服务监听从 `0.0.0.0` 改为 `127.0.0.1`（禁止局域网暴露）。
2. **默认回调地址**：`g_CallBack_Url` 默认指向 `http://127.0.0.1:8080/callback`（支持通过 `ECHOLOOM_CALLBACK_URL` / `ECHOLOOM_HOOK_PORT` 覆盖）。
3. **群聊 @ 提及支持**：为 `/SendTextMsg` 增加 `atlist` 参数与 `<atuserlist>` XML 校验，透传至原生文本构造。
4. **启动器加固**：校验 `Weixin.exe == 4.1.10.27` 精确版本、避免误注入子进程、加载同目录独立命名的 `wx_agent_hook.dll`（不与 `System32\version.dll` 重名）、支持 `ECHOLOOM_WEIXIN_EXE` 自定义安装路径及无弹窗 `--start` 模式。

## 3. 一键构建全部组件

在仓库根目录执行：

```powershell
.\hooks\wechat-4.1.10.27\build-wechat-hook.ps1
```

脚本会自动完成：
1. 拉取上游固定提交并校验完整性，打上上述补丁；
2. 用 MSBuild 编译 `.runtime/wechat-hook/wx_agent_hook.dll` 与 `WeChatHookLoader.exe`（端口 `:30001`）；
3. 用 `cl.exe` + 上游内含 MinHook 编译原图 CDN 主动下载扩展 `.runtime/native-image-download/wx_image_download_411027.dll`（端口 `:30002`，仅在 `Weixin.dll` 指纹匹配时生效）；
4. 将 `frida==17.26.1` 隔离安装到 `.runtime/native-image-download-deps`（供 `:30002` 原图下载与 `:30003` B 站长卡片 / 拍一拍接收扩展使用；不需要扩展可加 `-SkipNativeImage -SkipFrida`）；
5. 自动打印各产物 SHA-256 与可直接粘贴的 `[wechat_hook]` 配置段。

> 离线 / 代理环境可先手动下载上游提交 ZIP，再通过 `-UpstreamZip <zip路径>` 指定本地压缩包构建。

## 4. 接入 EchoLoom 并启动

1. 在 `.local/sources.toml` 启用 `wechat` 连接器（参考 [`config/sources.example.toml`](../../config/sources.example.toml)）：
   ```toml
   default_source = "wechat"

   [sources.wechat]
   enabled = true
   factory = "wechat_agent.connectors.wechat_hook:WeChatHookConnector"
   channel = "wechat"
   base_url = "http://127.0.0.1:30001"
   native_download_url = "http://127.0.0.1:30002"
   send_enabled = true
   ```
2. 将构建脚本打印的 `[wechat_hook]` 配置段（含路径、版本号和 SHA-256）粘贴到 `.local/runtime.toml`，并填好 `[identity]` 账号与群会话 ID（若微信不在 `C:\Program Files\Tencent\Weixin\Weixin.exe`，直接改 `weixin_exe` 即可）。
3. 启动全套服务：
   ```powershell
   .\scripts\start-services.ps1
   ```
   服务就绪后将分别监听：基础 Hook `:30001`、原图下载扩展 `:30002`、长卡片/拍一拍扩展 `:30003`、Agent Ingress `:8080`、只读观察台 `:8767`。

## 合规与许可说明

- 本目录中的构建脚本、补丁文件与 `native_image_download.cpp` 随 EchoLoom 采用 [0BSD](../../LICENSE) 开源。
- 上游 [`mosheng20205/WechatHook4.1.10.27`](https://github.com/mosheng20205/WechatHook4.1.10.27) 在 GitHub 未声明开源协议，EchoLoom 不分发其源码或二进制；使用者自行拉取编译运行并承担相关平台条款与合规责任。
