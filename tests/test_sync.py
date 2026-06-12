"""Tests for clickup-yaml-sync feat/multi-tag-milestone-backup.

Covers:
  - multi-tag push (preserve user-added UI tags, strip stale managed tags)
  - milestone_label -> lowercase tag slug
  - Epic dropdown custom-field push payload
  - backup-before-push writes a snapshot before any destructive call
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from unittest import mock

import pytest
import yaml

# Make ``import clickup`` work without packaging.
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

# Tests must NEVER hit the real network or read a real token from
# ~/bin/clickup.env. Stamp env before importing clickup so its lazy token
# loader returns this fake string consistently.
os.environ["CLICKUP_API_TOKEN"] = "pk_test_fake_token"
os.environ.setdefault("OPENAI_API_KEY", "sk_test_fake_key")

import clickup  # noqa: E402  (sys.path manipulation precedes import by design)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _epic_with(name: str, stories: list[dict], **extra) -> dict:
    base = {
        "number": 1,
        "name": name,
        "status": "in_progress",
        "points": 0,
        "stories": stories,
    }
    base.update(extra)
    return base


def _story_with(name: str, **extra) -> dict:
    base: dict = {"name": name, "status": "backlog", "points": 0}
    base.update(extra)
    return base


def _cu_task(task_id: str, tag_names: list[str], custom_fields: list[dict] | None = None) -> dict:
    return {
        "id": task_id,
        "custom_id": None,
        "name": "remote name",
        "status": {"status": "backlog"},
        "description": "",
        "priority": None,
        "tags": [{"name": t} for t in tag_names],
        "custom_fields": custom_fields or [],
        "custom_item_id": 0,
    }


def _data_with(stories_per_epic: dict[str, list[dict]], project_extra: dict | None = None) -> dict:
    """Build a minimal corpus-style YAML dict with multiple epics."""
    project = {"name": "test", "clickup_list_id": "999999"}
    if project_extra:
        project.update(project_extra)
    return {
        "project": project,
        "status_map": {"backlog": "backlog", "done": "done", "in_progress": "current sprint"},
        "epics": [
            _epic_with(name, stories) for name, stories in stories_per_epic.items()
        ],
    }


# ---------------------------------------------------------------------------
# 1. Multi-tag computation
# ---------------------------------------------------------------------------


class TestDesiredTags:
    def test_epic_name_always_present(self):
        epic = _epic_with("Infrastructure + CRM", [])
        story = _story_with("s1")
        assert clickup._story_desired_tags(story, epic) == ["Infrastructure + CRM"]

    def test_milestone_label_appends_lowercase_slug(self):
        epic = _epic_with("Kickoff / Access", [])
        story = _story_with("s1", milestone_label="M0")
        tags = clickup._story_desired_tags(story, epic)
        assert tags == ["Kickoff / Access", "m0"]

    def test_explicit_tags_merged_dedup_case_insensitive(self):
        epic = _epic_with("Infra", [])
        story = _story_with(
            "s1",
            milestone_label="M1",
            tags=["infra", "security", "M1"],  # dup of epic (case-insensitive) + dup of milestone
        )
        tags = clickup._story_desired_tags(story, epic)
        # epic first, then unique explicit tags (preserving order), milestone last if not already there
        assert tags == ["Infra", "security", "M1"]
        # "infra" was deduped against "Infra"; "M1" stayed; "m1" deduped against "M1"
        assert len(tags) == len(set(t.lower() for t in tags))

    def test_no_milestone_label_no_extra_tag(self):
        epic = _epic_with("Magnit", [])
        story = _story_with("s1", tags=["llm"])
        assert clickup._story_desired_tags(story, epic) == ["Magnit", "llm"]

    def test_invalid_tags_entries_ignored(self):
        epic = _epic_with("E", [])
        story = _story_with("s1", tags=["good", 42, None])
        assert clickup._story_desired_tags(story, epic) == ["E", "good"]

    def test_sprint_target_appends_s_tag(self):
        epic = _epic_with("Infra", [])
        story = _story_with("s1", sprint_target=2)
        assert clickup._story_desired_tags(story, epic) == ["Infra", "s2"]

    def test_description_meta_prefix_and_roundtrip(self):
        story = {"name": "x", "description": "Do the thing.", "points": 5,
                 "milestone_label": "M2", "sprint_target": 4}
        out = clickup.description_with_meta(story)
        assert out.startswith("Points: 5 · Milestone: M2 · Sprint: s4")
        assert "Do the thing." in out
        # compare_task must not flag a diff when ClickUp holds the meta'd body
        cu = {"name": "x", "description": out, "status": {"status": "backlog"}}
        diffs = clickup.compare_task(story, cu, {"backlog": "backlog"})
        assert not any(d["field"] == "description" for d in diffs)
        # no meta fields → plain body, no prefix
        assert clickup.description_with_meta({"description": "bare"}) == "bare"

    def test_push_epic_tag_false_omits_epic_name(self):
        epic = _epic_with("Infrastructure + CRM", [])
        story = _story_with("s1", milestone_label="M2", sprint_target=4, tags=["security"])
        # epic dropdown owns workstream → epic name tag suppressed
        assert clickup._story_desired_tags(story, epic, push_epic_tag=False) == [
            "security", "m2", "s4",
        ]
        # default still includes the epic name (backwards-compatible)
        assert clickup._story_desired_tags(story, epic)[0] == "Infrastructure + CRM"

    def test_sprint_target_with_milestone_and_tags(self):
        epic = _epic_with("Infra", [])
        story = _story_with(
            "s1",
            milestone_label="M1",
            sprint_target=3,
            tags=["llm"],
        )
        # epic + explicit tags + milestone slug + sprint slug, in that order
        assert clickup._story_desired_tags(story, epic) == ["Infra", "llm", "m1", "s3"]

    def test_sprint_target_none_or_zero_no_tag(self):
        epic = _epic_with("Infra", [])
        for bad in (None, 0, -1, "1", 1.5):
            story = _story_with("s1", sprint_target=bad)
            tags = clickup._story_desired_tags(story, epic)
            assert all(not t.startswith("s") or t.lower() == "security" for t in tags), (
                f"sprint_target={bad!r} unexpectedly produced an s-tag: {tags}"
            )

    def test_sprint_target_deduped_against_explicit(self):
        epic = _epic_with("Infra", [])
        story = _story_with("s1", sprint_target=4, tags=["S4"])  # case-insensitive dup
        tags = clickup._story_desired_tags(story, epic)
        assert tags == ["Infra", "S4"]


class TestManagedUniverseIncludesSprintSlugs:
    def test_sprint_slug_added_from_story_sprint_target(self):
        data = _data_with({
            "Infra": [_story_with("a", sprint_target=2)],
        })
        universe = clickup._collect_managed_tag_universe(data)
        assert "s2" in universe

    def test_sprint_slug_added_from_sprints_registry(self):
        data = _data_with({"Infra": [_story_with("a")]})
        data["sprints"] = [
            {"number": 1, "name": "Foundation", "start": "2026-06-01"},
            {"number": 5, "name": "Magnit", "start": "2026-06-29"},
        ]
        universe = clickup._collect_managed_tag_universe(data)
        assert "s1" in universe
        assert "s5" in universe


class TestManagedTagUniverse:
    def test_universe_includes_epic_names_milestones_and_user_tags(self):
        data = _data_with({
            "Kickoff / Access": [_story_with("a", milestone_label="M0", tags=["security"])],
            "Magnit": [_story_with("b", tags=["llm", "slack"])],
        })
        universe = clickup._collect_managed_tag_universe(data)
        # All names lowercased
        assert "kickoff / access" in universe
        assert "magnit" in universe
        assert "security" in universe
        assert "llm" in universe
        assert "slack" in universe
        # Milestone slugs always present (even if not used yet)
        for slug in ("m0", "m1", "m2", "m3"):
            assert slug in universe


# ---------------------------------------------------------------------------
# 2. _sync_tags — additive, preserves UI-added tags, strips managed-stale
# ---------------------------------------------------------------------------


class TestSyncTagsAdditive:
    def test_adds_missing_desired_tags(self):
        cu_task = _cu_task("T1", tag_names=[])
        with mock.patch.object(clickup, "clickup_add_tag") as add, \
             mock.patch.object(clickup, "clickup_remove_tag") as rm:
            clickup._sync_tags("tok", "T1", cu_task, ["epic-a", "m1"])
        assert sorted(c.args[2] for c in add.call_args_list) == ["epic-a", "m1"]
        rm.assert_not_called()

    def test_preserves_user_added_tags_not_in_managed_universe(self):
        """User added 'wip' in the UI; YAML didn't ask for it and doesn't manage it -> preserved."""
        cu_task = _cu_task("T1", tag_names=["epic-a", "wip"])
        with mock.patch.object(clickup, "clickup_add_tag") as add, \
             mock.patch.object(clickup, "clickup_remove_tag") as rm:
            clickup._sync_tags(
                "tok", "T1", cu_task,
                desired_tags=["epic-a", "m1"],
                managed_known_tags={"epic-a", "epic-b", "m0", "m1", "m2", "m3"},
            )
        add.assert_called_once_with("tok", "T1", "m1")
        # 'wip' is NOT in managed universe -> never removed
        for call in rm.call_args_list:
            assert call.args[2] != "wip"

    def test_strips_managed_stale_tags(self):
        """User reassigned the story to a different epic in YAML; old epic tag is stale."""
        cu_task = _cu_task("T1", tag_names=["epic-a", "m0"])
        with mock.patch.object(clickup, "clickup_add_tag") as add, \
             mock.patch.object(clickup, "clickup_remove_tag") as rm:
            clickup._sync_tags(
                "tok", "T1", cu_task,
                desired_tags=["epic-b", "m1"],
                managed_known_tags={"epic-a", "epic-b", "m0", "m1"},
            )
        # epic-b and m1 added; epic-a and m0 removed (both in managed universe, not desired)
        assert sorted(c.args[2] for c in add.call_args_list) == ["epic-b", "m1"]
        removed = sorted(c.args[2] for c in rm.call_args_list)
        assert removed == ["epic-a", "m0"]

    def test_strips_legacy_E_pattern_tags(self):
        """Pre-multi-tag schema used E1, E2 ... — always strip these."""
        cu_task = _cu_task("T1", tag_names=["E1", "E99", "epic-a"])
        with mock.patch.object(clickup, "clickup_add_tag"), \
             mock.patch.object(clickup, "clickup_remove_tag") as rm:
            clickup._sync_tags(
                "tok", "T1", cu_task, desired_tags=["epic-a"], managed_known_tags=set(),
            )
        removed = sorted(c.args[2] for c in rm.call_args_list)
        assert removed == ["E1", "E99"]

    def test_case_insensitive_match_does_not_re_add(self):
        cu_task = _cu_task("T1", tag_names=["Epic-A"])
        with mock.patch.object(clickup, "clickup_add_tag") as add, \
             mock.patch.object(clickup, "clickup_remove_tag") as rm:
            clickup._sync_tags("tok", "T1", cu_task, desired_tags=["epic-a"])
        add.assert_not_called()
        rm.assert_not_called()


# ---------------------------------------------------------------------------
# 3. Custom dropdown field push
# ---------------------------------------------------------------------------


class TestEpicDropdownPush:
    PROJECT_CFG = {
        "epic_dropdown_field_id": "FIELD-UUID",
        "epic_dropdown_options": {
            "Kickoff / Access": "OPT-KICKOFF",
            "Magnit Monitoring System": "OPT-MAGNIT",
        },
    }

    def test_skips_when_no_field_id_configured(self):
        epic = _epic_with("Magnit Monitoring System", [])
        story = _story_with("s1")
        with mock.patch.object(clickup, "clickup_set_custom_field") as set_cf:
            attempted = clickup._push_epic_dropdown_if_needed(
                "tok", "T1", _cu_task("T1", []), story, epic, project_cfg={}, dry_run=False
            )
        assert attempted is False
        set_cf.assert_not_called()

    def test_pushes_matching_option_id(self):
        epic = _epic_with("Magnit Monitoring System", [])
        story = _story_with("s1")
        cu_task = _cu_task("T1", [])
        with mock.patch.object(clickup, "clickup_set_custom_field") as set_cf:
            attempted = clickup._push_epic_dropdown_if_needed(
                "tok", "T1", cu_task, story, epic, project_cfg=self.PROJECT_CFG, dry_run=False
            )
        assert attempted is True
        set_cf.assert_called_once_with("tok", "T1", "FIELD-UUID", "OPT-MAGNIT")

    def test_case_insensitive_option_lookup(self):
        epic = _epic_with("magnit monitoring system", [])
        story = _story_with("s1")
        cu_task = _cu_task("T1", [])
        with mock.patch.object(clickup, "clickup_set_custom_field") as set_cf:
            clickup._push_epic_dropdown_if_needed(
                "tok", "T1", cu_task, story, epic, project_cfg=self.PROJECT_CFG, dry_run=False
            )
        set_cf.assert_called_once()
        assert set_cf.call_args.args[3] == "OPT-MAGNIT"

    def test_story_override_wins(self):
        epic = _epic_with("Kickoff / Access", [])
        story = _story_with("s1", epic_dropdown_value="Magnit Monitoring System")
        cu_task = _cu_task("T1", [])
        with mock.patch.object(clickup, "clickup_set_custom_field") as set_cf:
            clickup._push_epic_dropdown_if_needed(
                "tok", "T1", cu_task, story, epic, project_cfg=self.PROJECT_CFG, dry_run=False
            )
        set_cf.assert_called_once()
        assert set_cf.call_args.args[3] == "OPT-MAGNIT"

    def test_noop_when_value_already_matches(self):
        epic = _epic_with("Magnit Monitoring System", [])
        story = _story_with("s1")
        cu_task = _cu_task("T1", [], custom_fields=[{"id": "FIELD-UUID", "value": "OPT-MAGNIT"}])
        with mock.patch.object(clickup, "clickup_set_custom_field") as set_cf:
            attempted = clickup._push_epic_dropdown_if_needed(
                "tok", "T1", cu_task, story, epic, project_cfg=self.PROJECT_CFG, dry_run=False
            )
        assert attempted is False
        set_cf.assert_not_called()

    def test_unknown_option_name_is_skipped(self):
        epic = _epic_with("Mystery Epic", [])
        story = _story_with("s1")
        with mock.patch.object(clickup, "clickup_set_custom_field") as set_cf:
            attempted = clickup._push_epic_dropdown_if_needed(
                "tok", "T1", _cu_task("T1", []), story, epic, project_cfg=self.PROJECT_CFG, dry_run=False
            )
        assert attempted is False
        set_cf.assert_not_called()

    def test_dry_run_does_not_call_api(self):
        epic = _epic_with("Magnit Monitoring System", [])
        story = _story_with("s1")
        cu_task = _cu_task("T1", [])
        with mock.patch.object(clickup, "clickup_set_custom_field") as set_cf:
            attempted = clickup._push_epic_dropdown_if_needed(
                "tok", "T1", cu_task, story, epic, project_cfg=self.PROJECT_CFG, dry_run=True
            )
        # Reports it would have written, but doesn't actually call.
        assert attempted is True
        set_cf.assert_not_called()


# ---------------------------------------------------------------------------
# 4. clickup_set_custom_field — payload shape
# ---------------------------------------------------------------------------


class TestCustomFieldPayloadShape:
    def test_post_body_value_key(self):
        """The wire request must be POST /task/{id}/field/{fid} with {'value': ...}."""
        with mock.patch.object(clickup, "_api_request") as api:
            api.return_value = {}
            clickup.clickup_set_custom_field("tok", "TASK-1", "FIELD-1", "OPT-X")
        api.assert_called_once()
        method, url, token = api.call_args.args[:3]
        body = api.call_args.args[3]
        assert method == "POST"
        assert url.endswith("/task/TASK-1/field/FIELD-1")
        assert token == "tok"
        assert body == {"value": "OPT-X"}


# ---------------------------------------------------------------------------
# 5. Backup-before-push
# ---------------------------------------------------------------------------


class TestBackupBeforePush:
    @staticmethod
    def _fixture(tmp_path):
        yaml_data = _data_with({
            "Kickoff / Access": [
                _story_with("s1", clickup_id="T1", milestone_label="M0", tags=["security"]),
            ],
        })
        yaml_path = tmp_path / "project.yaml"
        with open(yaml_path, "w") as f:
            yaml.safe_dump(yaml_data, f)
        return yaml_data, yaml_path

    def test_backup_written_before_any_modifying_call(self, tmp_path):
        yaml_data, yaml_path = self._fixture(tmp_path)
        backup_path = tmp_path / "backup.yaml"

        cu_tasks = [_cu_task("T1", ["Kickoff / Access"])]

        call_log: list[str] = []

        def _fake_list(token, list_id, page=0):
            call_log.append("list")
            return cu_tasks

        def _fake_update(token, task_id, body):
            call_log.append(f"update:{task_id}")
            return {}

        def _fake_add_tag(token, task_id, name):
            call_log.append(f"add_tag:{task_id}:{name}")
            return {}

        def _fake_rm_tag(token, task_id, name):
            call_log.append(f"rm_tag:{task_id}:{name}")
            return {}

        # Spy on the backup write itself rather than monkey-patching open()
        # — _maybe_write_backup is the canonical "destructive ops are about
        # to happen, snapshot first" boundary.
        real_backup = clickup._maybe_write_backup

        def _spy_backup(**kwargs):
            call_log.append("backup_written")
            return real_backup(**kwargs)

        with mock.patch.object(clickup, "clickup_list_tasks", side_effect=_fake_list), \
             mock.patch.object(clickup, "clickup_update_task", side_effect=_fake_update), \
             mock.patch.object(clickup, "clickup_add_tag", side_effect=_fake_add_tag), \
             mock.patch.object(clickup, "clickup_remove_tag", side_effect=_fake_rm_tag), \
             mock.patch.object(clickup, "clickup_set_custom_field"), \
             mock.patch.object(clickup, "_maybe_write_backup", side_effect=_spy_backup):

            clickup.cmd_push(
                yaml_data, str(yaml_path),
                dry_run=False,
                backup_path=str(backup_path),
                backup_default=True,
            )

        # 1. Backup file exists and is valid YAML containing the remote task.
        assert backup_path.exists(), "backup file was not written"
        snap = yaml.safe_load(backup_path.read_text())
        assert "epics" in snap
        story_names = [
            s["name"] for e in snap["epics"] for s in e.get("stories", [])
        ]
        assert "remote name" in story_names

        # 2. The backup write happened BEFORE any update/add_tag/rm_tag call.
        first_modify = next(
            (i for i, ev in enumerate(call_log)
             if ev.startswith(("update:", "add_tag:", "rm_tag:"))),
            None,
        )
        first_backup = call_log.index("backup_written") if "backup_written" in call_log else None
        assert first_backup is not None, "backup was never invoked"
        if first_modify is not None:
            assert first_backup < first_modify, (
                f"backup must precede any modifying call; saw {call_log}"
            )

    def test_no_backup_skips_file(self, tmp_path):
        yaml_data, yaml_path = self._fixture(tmp_path)
        cu_tasks = [_cu_task("T1", ["Kickoff / Access"])]
        with mock.patch.object(clickup, "clickup_list_tasks", return_value=cu_tasks), \
             mock.patch.object(clickup, "clickup_update_task"), \
             mock.patch.object(clickup, "clickup_add_tag"), \
             mock.patch.object(clickup, "clickup_remove_tag"), \
             mock.patch.object(clickup, "clickup_set_custom_field"), \
             mock.patch.object(clickup, "_default_backup_path") as dbp:
            dbp.side_effect = AssertionError("should not be called")
            clickup.cmd_push(
                yaml_data, str(yaml_path),
                dry_run=False,
                backup_path=None,
                backup_default=False,
            )

    def test_empty_list_skips_default_backup(self, tmp_path):
        """Brand-new sandbox list: nothing to back up, skip even with default on."""
        yaml_data, yaml_path = self._fixture(tmp_path)
        with mock.patch.object(clickup, "clickup_list_tasks", return_value=[]), \
             mock.patch.object(clickup, "clickup_create_task", return_value={"id": "NEW", "custom_id": None}), \
             mock.patch.object(clickup, "clickup_set_custom_field"), \
             mock.patch.object(clickup, "_default_backup_path") as dbp, \
             mock.patch.object(clickup, "save_yaml"):
            dbp.side_effect = AssertionError("default backup must not be invoked on empty list")
            clickup.cmd_push(
                yaml_data, str(yaml_path),
                dry_run=False,
                backup_path=None,  # rely on default
                backup_default=True,
            )

    def test_dry_run_does_not_write_backup(self, tmp_path):
        yaml_data, yaml_path = self._fixture(tmp_path)
        backup_path = tmp_path / "backup.yaml"
        with mock.patch.object(clickup, "clickup_list_tasks", return_value=[_cu_task("T1", [])]):
            clickup.cmd_push(
                yaml_data, str(yaml_path),
                dry_run=True,
                backup_path=str(backup_path),
                backup_default=True,
            )
        assert not backup_path.exists()


# ---------------------------------------------------------------------------
# 6. End-to-end push smoke: multi-tag + milestone + dropdown together
# ---------------------------------------------------------------------------


class TestPushIntegration:
    def test_create_path_emits_multi_tag_body_and_dropdown_call(self, tmp_path):
        data = _data_with(
            {
                "Magnit Monitoring System": [
                    _story_with(
                        "Magnit ingestion adapter",
                        milestone_label="M2",
                        tags=["magnit"],
                    )
                ],
            },
            project_extra={
                "epic_dropdown_field_id": "FIELD-UUID",
                "epic_dropdown_options": {
                    "Magnit Monitoring System": "OPT-MAGNIT",
                },
            },
        )
        yaml_path = tmp_path / "p.yaml"
        with open(yaml_path, "w") as f:
            yaml.safe_dump(data, f)

        created: dict = {}

        def _fake_create(token, list_id, body):
            created["body"] = body
            return {"id": "NEW-1", "custom_id": "TASK-NEW-1"}

        with mock.patch.object(clickup, "clickup_list_tasks", return_value=[]), \
             mock.patch.object(clickup, "clickup_create_task", side_effect=_fake_create), \
             mock.patch.object(clickup, "clickup_set_custom_field") as set_cf, \
             mock.patch.object(clickup, "save_yaml"):
            stats = clickup.cmd_push(
                data, str(yaml_path),
                dry_run=False,
                backup_path=None,
                backup_default=False,
            )

        assert stats["created"] == 1
        # All three sources merged on create:
        assert created["body"]["tags"] == ["Magnit Monitoring System", "magnit", "m2"]
        # Epic dropdown pushed in the same push call:
        set_cf.assert_called_once()
        assert set_cf.call_args.args[1] == "NEW-1"
        assert set_cf.call_args.args[2] == "FIELD-UUID"
        assert set_cf.call_args.args[3] == "OPT-MAGNIT"


# ---------------------------------------------------------------------------
# 7. Assignees: resolution + bidirectional reconcile
# ---------------------------------------------------------------------------

MEMBERS = [
    {"id": 100, "username": "Kathy Jung", "email": "kathy@e-m-marketing.com"},
    {"id": 200, "username": "Charlie Mock", "email": "charliem@e-m-marketing.com"},
    {"id": 300, "username": "Maurice McCabe", "email": "maurice@spark6.com"},
]


@pytest.fixture(autouse=True)
def _no_real_member_fetch():
    """cmd_push/sync/diff now fetch list members; default to an empty roster so
    no test accidentally hits the network. Tests that exercise assignees patch
    clickup_get_list_members explicitly to override this."""
    with mock.patch.object(clickup, "clickup_get_list_members", return_value=[]):
        yield


def _cu_task_assignees(task_id: str, id_emails: list[tuple[int, str]]) -> dict:
    base = _cu_task(task_id, [])
    base["assignees"] = [
        {"id": uid, "username": f"u{uid}", "email": email}
        for uid, email in id_emails
    ]
    return base


class TestAssigneeResolver:
    def test_resolves_email_username_and_id(self):
        r = clickup._build_assignee_resolver(MEMBERS)
        ids, unresolved = clickup._resolve_assignee_ids(
            ["kathy@e-m-marketing.com", "Charlie Mock", "300"], r
        )
        assert ids == [100, 200, 300]
        assert unresolved == []

    def test_case_insensitive_and_dedup(self):
        r = clickup._build_assignee_resolver(MEMBERS)
        ids, unresolved = clickup._resolve_assignee_ids(
            ["KATHY@E-M-MARKETING.COM", "kathy@e-m-marketing.com"], r
        )
        assert ids == [100]
        assert unresolved == []

    def test_unknown_is_reported_not_fatal(self):
        r = clickup._build_assignee_resolver(MEMBERS)
        ids, unresolved = clickup._resolve_assignee_ids(["nobody@x.com"], r)
        assert ids == []
        assert unresolved == ["nobody@x.com"]


class TestSyncAssignees:
    def setup_method(self):
        self.r = clickup._build_assignee_resolver(MEMBERS)

    def test_absent_key_is_unmanaged_noop(self):
        story = _story_with("s", clickup_id="T1")
        cu = _cu_task_assignees("T1", [(100, "kathy@e-m-marketing.com")])
        with mock.patch.object(clickup, "clickup_update_task") as upd:
            assert clickup._sync_assignees("tok", "T1", cu, story, self.r) is False
        upd.assert_not_called()

    def test_adds_and_removes_to_match_yaml(self):
        story = _story_with("s", clickup_id="T1", assignees=["charliem@e-m-marketing.com"])
        cu = _cu_task_assignees("T1", [(100, "kathy@e-m-marketing.com")])
        with mock.patch.object(clickup, "clickup_update_task") as upd:
            assert clickup._sync_assignees("tok", "T1", cu, story, self.r) is True
        assert upd.call_args.args[2] == {"assignees": {"add": [200], "rem": [100]}}

    def test_empty_list_clears(self):
        story = _story_with("s", clickup_id="T1", assignees=[])
        cu = _cu_task_assignees("T1", [(100, "kathy@e-m-marketing.com")])
        with mock.patch.object(clickup, "clickup_update_task") as upd:
            assert clickup._sync_assignees("tok", "T1", cu, story, self.r) is True
        assert upd.call_args.args[2] == {"assignees": {"add": [], "rem": [100]}}

    def test_no_change_when_already_matches(self):
        story = _story_with("s", clickup_id="T1", assignees=["kathy@e-m-marketing.com"])
        cu = _cu_task_assignees("T1", [(100, "kathy@e-m-marketing.com")])
        with mock.patch.object(clickup, "clickup_update_task") as upd:
            assert clickup._sync_assignees("tok", "T1", cu, story, self.r) is False
        upd.assert_not_called()


class TestPullAssignees:
    def test_reads_remote_into_yaml_sorted(self):
        story = _story_with("s", clickup_id="T1")
        cu = _cu_task_assignees("T1", [
            (200, "charliem@e-m-marketing.com"), (100, "kathy@e-m-marketing.com")
        ])
        assert clickup._pull_assignees(story, cu) is True
        assert story["assignees"] == [
            "charliem@e-m-marketing.com", "kathy@e-m-marketing.com"
        ]

    def test_remote_removal_becomes_empty_list_when_managed(self):
        story = _story_with("s", clickup_id="T1", assignees=["kathy@e-m-marketing.com"])
        cu = _cu_task_assignees("T1", [])
        assert clickup._pull_assignees(story, cu) is True
        assert story["assignees"] == []

    def test_no_litter_when_both_empty(self):
        story = _story_with("s", clickup_id="T1")
        cu = _cu_task_assignees("T1", [])
        assert clickup._pull_assignees(story, cu) is False
        assert "assignees" not in story

    def test_idempotent_when_equal(self):
        story = _story_with("s", clickup_id="T1", assignees=["kathy@e-m-marketing.com"])
        cu = _cu_task_assignees("T1", [(100, "kathy@e-m-marketing.com")])
        assert clickup._pull_assignees(story, cu) is False


class TestBuildTaskBodyAssignees:
    def test_create_body_includes_ids(self):
        body = clickup.build_task_body(
            _story_with("s"), {"backlog": "backlog"}, assignee_ids=[100, 200]
        )
        assert body["assignees"] == [100, 200]

    def test_create_body_omits_when_empty(self):
        body = clickup.build_task_body(
            _story_with("s"), {"backlog": "backlog"}, assignee_ids=[]
        )
        assert "assignees" not in body


class TestPushAssigneesIntegration:
    def test_create_resolves_emails_to_ids(self, tmp_path):
        data = _data_with({
            "Kickoff / Access": [
                _story_with("new task", assignees=["kathy@e-m-marketing.com"]),
            ],
        })
        yaml_path = tmp_path / "p.yaml"
        with open(yaml_path, "w") as f:
            yaml.safe_dump(data, f)
        created: dict = {}

        def _fake_create(token, list_id, body):
            created["body"] = body
            return {"id": "NEW-1", "custom_id": None}

        with mock.patch.object(clickup, "clickup_list_tasks", return_value=[]), \
             mock.patch.object(clickup, "clickup_get_list_members", return_value=MEMBERS), \
             mock.patch.object(clickup, "clickup_create_task", side_effect=_fake_create), \
             mock.patch.object(clickup, "save_yaml"):
            clickup.cmd_push(data, str(yaml_path), dry_run=False,
                             backup_path=None, backup_default=False)
        assert created["body"]["assignees"] == [100]

    def test_update_reconciles_to_match_yaml(self, tmp_path):
        data = _data_with({
            "Kickoff / Access": [
                _story_with("existing", clickup_id="T1",
                            assignees=["charliem@e-m-marketing.com"]),
            ],
        })
        yaml_path = tmp_path / "p.yaml"
        with open(yaml_path, "w") as f:
            yaml.safe_dump(data, f)
        cu = _cu_task_assignees("T1", [(100, "kathy@e-m-marketing.com")])

        with mock.patch.object(clickup, "clickup_list_tasks", return_value=[cu]), \
             mock.patch.object(clickup, "clickup_get_list_members", return_value=MEMBERS), \
             mock.patch.object(clickup, "clickup_update_task") as upd, \
             mock.patch.object(clickup, "clickup_add_tag"), \
             mock.patch.object(clickup, "clickup_remove_tag"), \
             mock.patch.object(clickup, "save_yaml"):
            clickup.cmd_push(data, str(yaml_path), dry_run=False,
                             backup_path=None, backup_default=False)
        assignee_calls = [c for c in upd.call_args_list if "assignees" in c.args[2]]
        assert len(assignee_calls) == 1
        assert assignee_calls[0].args[2]["assignees"] == {"add": [200], "rem": [100]}


class TestPullAssigneesDryRunPreview:
    def test_target_is_pure_and_matches_apply(self):
        # _assignees_pull_target must NOT mutate, and must agree with _pull_assignees.
        story = _story_with("s", clickup_id="T1")
        cu = _cu_task_assignees("T1", [(100, "kathy@e-m-marketing.com")])
        target = clickup._assignees_pull_target(story, cu)
        assert target == ["kathy@e-m-marketing.com"]
        assert "assignees" not in story  # not mutated by the preview
        # Applying yields the same value the preview reported.
        assert clickup._pull_assignees(story, cu) is True
        assert story["assignees"] == target

    def test_target_none_when_equal(self):
        story = _story_with("s", clickup_id="T1", assignees=["kathy@e-m-marketing.com"])
        cu = _cu_task_assignees("T1", [(100, "kathy@e-m-marketing.com")])
        assert clickup._assignees_pull_target(story, cu) is None

    def test_dry_run_pull_does_not_write_assignees(self, tmp_path):
        data = _data_with({
            "Kickoff / Access": [_story_with("s", clickup_id="T1")],
        })
        yaml_path = tmp_path / "p.yaml"
        with open(yaml_path, "w") as f:
            yaml.safe_dump(data, f)
        cu = _cu_task_assignees("T1", [(100, "kathy@e-m-marketing.com")])
        with mock.patch.object(clickup, "clickup_list_tasks", return_value=[cu]), \
             mock.patch.object(clickup, "save_yaml") as save:
            clickup.cmd_pull(data, str(yaml_path), dry_run=True)
        # Dry run must not mutate the in-memory story nor save.
        assert "assignees" not in data["epics"][0]["stories"][0]
        save.assert_not_called()
