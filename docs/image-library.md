# 图片与表情图库

所有图片归入同一个图库，包含聊天收到的图片、Agent 生成/发送的图片和人工导入的图片。完整图库查询会返回图片描述和来源聊天。

每轮聊天请求末尾会动态列出最多 10 张已启用的快速发送图片，按 `display_order` 排序。该清单包含稳定 `image_id` 和简短描述。Agent 可用 `list_wechat_images` 查看最近图片或完整目录，用 `manage_wechat_image_library` 修改描述、是否进入提示词清单和展示顺序；选定后用 `send_wechat_image` 发送。

## 人工管理

在仓库根目录运行：

```powershell
.\.venv\Scripts\python.exe scripts\image-library.py list
.\.venv\Scripts\python.exe scripts\image-library.py import .\tmp\stickers
.\.venv\Scripts\python.exe scripts\image-library.py set img_完整ID --description "猫猫害羞表情" --quick-send yes --order 10
```

`import` 接受单个 PNG/JPG/GIF/WebP 文件或目录，默认导入图片并加入快速发送清单；添加 `--hide-from-prompt` 可先只归档。每张图片最大 8 MB，描述最多 300 字，提示词清单最多展示 10 张。单账号数据库可省略 `--account-id`；多账号时必须指定 wxid。

项目内管理入口和 Agent 工具更新的是同一份图库元数据；人工导入、描述、启用状态和排序修改均写入图片库修订记录。
