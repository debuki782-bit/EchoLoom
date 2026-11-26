import subprocess
from pathlib import Path

from echoloom.project_tools import ProjectTools


def test_project_tool_is_scoped_and_prompt_edits_can_restore_git_head(tmp_path: Path):
    (tmp_path / "docs").mkdir()
    (tmp_path / "prompts").mkdir()
    (tmp_path / "data").mkdir()
    (tmp_path / "docs" / "guide.md").write_text("甲\n乙\n", encoding="utf-8")
    prompt = tmp_path / "prompts" / "persona.md"
    prompt.write_text("原本性格\n保持温柔", encoding="utf-8")
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "add", "prompts/persona.md"], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "-c", "user.name=Test", "-c",
                    "user.email=test@example.invalid", "commit", "-qm", "initial"], check=True)
    (tmp_path / "data" / "secret.txt").write_text("secret", encoding="utf-8")
    tools = ProjectTools(tmp_path)

    assert tools.list_project_resources("docs")["items"] == [{"name": "guide.md", "type": "file"}]
    assert tools.read_project_resource("docs/guide.md", 2, 1)["content"] == "乙"
    assert tools.read_project_resource("data/secret.txt")["status"] == "rejected"
    assert tools.read_project_resource("docs/../data/secret.txt")["status"] == "rejected"
    assert tools.update_own_prompt("../docs/guide.md", "", "甲", "丙")["status"] == "rejected"

    digest = tools.read_project_resource("prompts/persona.md")["sha256"]
    assert tools.update_own_prompt("persona.md", "wrong", "原本性格", "新性格")["status"] == "rejected"
    result = tools.update_own_prompt("persona.md", digest, "原本性格", "新性格")
    assert result["status"] == "updated"
    assert prompt.read_text(encoding="utf-8") == "新性格\n保持温柔"
    assert tools.update_own_prompt("persona.md", digest, "保持温柔", "改")['status'] == "rejected"
    revisions = tools.list_prompt_revisions("persona.md")
    assert revisions["status"] == "completed" and len(revisions["revisions"]) == 1
    commit = revisions["revisions"][0]["revision"]
    assert tools.rollback_own_prompt("persona.md", digest)["status"] == "rejected"
    assert tools.rollback_own_prompt("persona.md", result["sha256"], "--help")["status"] == "rejected"
    assert tools.rollback_own_prompt("persona.md", result["sha256"], commit)["status"] == "updated"
    assert prompt.read_text(encoding="utf-8") == "原本性格\n保持温柔"


def test_project_resource_rejects_non_text_and_non_unique_replacements(tmp_path: Path):
    (tmp_path / "prompts").mkdir()
    (tmp_path / "prompts" / "persona.md").write_text("重复 重复", encoding="utf-8")
    (tmp_path / "prompts" / "secret.sqlite").write_text("secret", encoding="utf-8")
    tools = ProjectTools(tmp_path)
    assert tools.read_project_resource("prompts/secret.sqlite")["status"] == "rejected"
    digest = tools.read_project_resource("prompts/persona.md")["sha256"]
    assert tools.update_own_prompt("persona.md", digest, "重复", "修改")["status"] == "rejected"


def test_project_tools_updates_local_prompt_without_touching_base_template(tmp_path: Path):
    (tmp_path / "prompts").mkdir()
    (tmp_path / "prompts.local").mkdir()
    (tmp_path / "prompts" / "persona.md").write_text("base persona", encoding="utf-8")
    (tmp_path / "prompts.local" / "persona.md").write_text("local v1", encoding="utf-8")

    tools = ProjectTools(tmp_path)
    read_result = tools.read_project_resource("prompts.local/persona.md")
    assert read_result["status"] != "rejected"
    digest = read_result["sha256"]

    update_result = tools.update_own_prompt("persona.md", digest, "local v1", "local v2")
    assert update_result["status"] != "rejected"
    assert (tmp_path / "prompts.local" / "persona.md").read_text(encoding="utf-8") == "local v2"
    assert (tmp_path / "prompts" / "persona.md").read_text(encoding="utf-8") == "base persona"

