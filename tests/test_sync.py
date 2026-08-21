"""Tests for clickup-yaml-sync feat/multi-tag-milestone-backup.

Covers:
  - multi-tag push (preserve user-added UI tags, strip stale managed tags)
  - milestone_label -> lowercase tag slug
  - Epic dropdown custom-field push payload
  - backup-before-push writes a snapshot before any destructive call
"""

from __future__ import annotations

import copy
import json
import os
import sys
import time
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
        epic = _epic_with("VendorX", [])
        story = _story_with("s1", tags=["llm"])
        assert clickup._story_desired_tags(story, epic) == ["VendorX", "llm"]

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
            {"number": 5, "name": "VendorX", "start": "2026-06-29"},
        ]
        universe = clickup._collect_managed_tag_universe(data)
        assert "s1" in universe
        assert "s5" in universe


class TestManagedTagUniverse:
    def test_universe_includes_epic_names_milestones_and_user_tags(self):
        data = _data_with({
            "Kickoff / Access": [_story_with("a", milestone_label="M0", tags=["security"])],
            "VendorX": [_story_with("b", tags=["llm", "slack"])],
        })
        universe = clickup._collect_managed_tag_universe(data)
        # All names lowercased
        assert "kickoff / access" in universe
        assert "vendorx" in universe
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
            "VendorX Monitoring System": "OPT-VENDORX",
        },
    }

    def test_skips_when_no_field_id_configured(self):
        epic = _epic_with("VendorX Monitoring System", [])
        story = _story_with("s1")
        with mock.patch.object(clickup, "clickup_set_custom_field") as set_cf:
            attempted = clickup._push_epic_dropdown_if_needed(
                "tok", "T1", _cu_task("T1", []), story, epic, project_cfg={}, dry_run=False
            )
        assert attempted is False
        set_cf.assert_not_called()

    def test_pushes_matching_option_id(self):
        epic = _epic_with("VendorX Monitoring System", [])
        story = _story_with("s1")
        cu_task = _cu_task("T1", [])
        with mock.patch.object(clickup, "clickup_set_custom_field") as set_cf:
            attempted = clickup._push_epic_dropdown_if_needed(
                "tok", "T1", cu_task, story, epic, project_cfg=self.PROJECT_CFG, dry_run=False
            )
        assert attempted is True
        set_cf.assert_called_once_with("tok", "T1", "FIELD-UUID", "OPT-VENDORX")

    def test_case_insensitive_option_lookup(self):
        epic = _epic_with("vendorx monitoring system", [])
        story = _story_with("s1")
        cu_task = _cu_task("T1", [])
        with mock.patch.object(clickup, "clickup_set_custom_field") as set_cf:
            clickup._push_epic_dropdown_if_needed(
                "tok", "T1", cu_task, story, epic, project_cfg=self.PROJECT_CFG, dry_run=False
            )
        set_cf.assert_called_once()
        assert set_cf.call_args.args[3] == "OPT-VENDORX"

    def test_story_override_wins(self):
        epic = _epic_with("Kickoff / Access", [])
        story = _story_with("s1", epic_dropdown_value="VendorX Monitoring System")
        cu_task = _cu_task("T1", [])
        with mock.patch.object(clickup, "clickup_set_custom_field") as set_cf:
            clickup._push_epic_dropdown_if_needed(
                "tok", "T1", cu_task, story, epic, project_cfg=self.PROJECT_CFG, dry_run=False
            )
        set_cf.assert_called_once()
        assert set_cf.call_args.args[3] == "OPT-VENDORX"

    def test_noop_when_value_already_matches(self):
        epic = _epic_with("VendorX Monitoring System", [])
        story = _story_with("s1")
        cu_task = _cu_task("T1", [], custom_fields=[{"id": "FIELD-UUID", "value": "OPT-VENDORX"}])
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
        epic = _epic_with("VendorX Monitoring System", [])
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
                "VendorX Monitoring System": [
                    _story_with(
                        "VendorX ingestion adapter",
                        milestone_label="M2",
                        tags=["vendorx"],
                    )
                ],
            },
            project_extra={
                "epic_dropdown_field_id": "FIELD-UUID",
                "epic_dropdown_options": {
                    "VendorX Monitoring System": "OPT-VENDORX",
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
        assert created["body"]["tags"] == ["VendorX Monitoring System", "vendorx", "m2"]
        # Epic dropdown pushed in the same push call:
        set_cf.assert_called_once()
        assert set_cf.call_args.args[1] == "NEW-1"
        assert set_cf.call_args.args[2] == "FIELD-UUID"
        assert set_cf.call_args.args[3] == "OPT-VENDORX"


# ---------------------------------------------------------------------------
# 7. Assignees: resolution + bidirectional reconcile
# ---------------------------------------------------------------------------

MEMBERS = [
    {"id": 100, "username": "Bob Example", "email": "bob@example.com"},
    {"id": 200, "username": "Alice Example", "email": "alice@example.com"},
    {"id": 300, "username": "Dave Example", "email": "dave@example.com"},
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
            ["bob@example.com", "Alice Example", "300"], r
        )
        assert ids == [100, 200, 300]
        assert unresolved == []

    def test_case_insensitive_and_dedup(self):
        r = clickup._build_assignee_resolver(MEMBERS)
        ids, unresolved = clickup._resolve_assignee_ids(
            ["BOB@EXAMPLE.COM", "bob@example.com"], r
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
        cu = _cu_task_assignees("T1", [(100, "bob@example.com")])
        with mock.patch.object(clickup, "clickup_update_task") as upd:
            assert clickup._sync_assignees("tok", "T1", cu, story, self.r) is False
        upd.assert_not_called()

    def test_adds_and_removes_to_match_yaml(self):
        story = _story_with("s", clickup_id="T1", assignees=["alice@example.com"])
        cu = _cu_task_assignees("T1", [(100, "bob@example.com")])
        with mock.patch.object(clickup, "clickup_update_task") as upd:
            assert clickup._sync_assignees("tok", "T1", cu, story, self.r) is True
        assert upd.call_args.args[2] == {"assignees": {"add": [200], "rem": [100]}}

    def test_empty_list_clears(self):
        story = _story_with("s", clickup_id="T1", assignees=[])
        cu = _cu_task_assignees("T1", [(100, "bob@example.com")])
        with mock.patch.object(clickup, "clickup_update_task") as upd:
            assert clickup._sync_assignees("tok", "T1", cu, story, self.r) is True
        assert upd.call_args.args[2] == {"assignees": {"add": [], "rem": [100]}}

    def test_no_change_when_already_matches(self):
        story = _story_with("s", clickup_id="T1", assignees=["bob@example.com"])
        cu = _cu_task_assignees("T1", [(100, "bob@example.com")])
        with mock.patch.object(clickup, "clickup_update_task") as upd:
            assert clickup._sync_assignees("tok", "T1", cu, story, self.r) is False
        upd.assert_not_called()


class TestPullAssignees:
    def test_reads_remote_into_yaml_sorted(self):
        story = _story_with("s", clickup_id="T1")
        cu = _cu_task_assignees("T1", [
            (200, "alice@example.com"), (100, "bob@example.com")
        ])
        assert clickup._pull_assignees(story, cu) is True
        assert story["assignees"] == [
            "alice@example.com", "bob@example.com"
        ]

    def test_remote_removal_becomes_empty_list_when_managed(self):
        story = _story_with("s", clickup_id="T1", assignees=["bob@example.com"])
        cu = _cu_task_assignees("T1", [])
        assert clickup._pull_assignees(story, cu) is True
        assert story["assignees"] == []

    def test_no_litter_when_both_empty(self):
        story = _story_with("s", clickup_id="T1")
        cu = _cu_task_assignees("T1", [])
        assert clickup._pull_assignees(story, cu) is False
        assert "assignees" not in story

    def test_idempotent_when_equal(self):
        story = _story_with("s", clickup_id="T1", assignees=["bob@example.com"])
        cu = _cu_task_assignees("T1", [(100, "bob@example.com")])
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
                _story_with("new task", assignees=["bob@example.com"]),
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
                            assignees=["alice@example.com"]),
            ],
        })
        yaml_path = tmp_path / "p.yaml"
        with open(yaml_path, "w") as f:
            yaml.safe_dump(data, f)
        cu = _cu_task_assignees("T1", [(100, "bob@example.com")])

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
        cu = _cu_task_assignees("T1", [(100, "bob@example.com")])
        target = clickup._assignees_pull_target(story, cu)
        assert target == ["bob@example.com"]
        assert "assignees" not in story  # not mutated by the preview
        # Applying yields the same value the preview reported.
        assert clickup._pull_assignees(story, cu) is True
        assert story["assignees"] == target

    def test_target_none_when_equal(self):
        story = _story_with("s", clickup_id="T1", assignees=["bob@example.com"])
        cu = _cu_task_assignees("T1", [(100, "bob@example.com")])
        assert clickup._assignees_pull_target(story, cu) is None

    def test_dry_run_pull_does_not_write_assignees(self, tmp_path):
        data = _data_with({
            "Kickoff / Access": [_story_with("s", clickup_id="T1")],
        })
        yaml_path = tmp_path / "p.yaml"
        with open(yaml_path, "w") as f:
            yaml.safe_dump(data, f)
        cu = _cu_task_assignees("T1", [(100, "bob@example.com")])
        with mock.patch.object(clickup, "clickup_list_tasks", return_value=[cu]), \
             mock.patch.object(clickup, "save_yaml") as save:
            clickup.cmd_pull(data, str(yaml_path), dry_run=True)
        # Dry run must not mutate the in-memory story nor save.
        assert "assignees" not in data["epics"][0]["stories"][0]
        save.assert_not_called()


# ---------------------------------------------------------------------------
# 3-way merge engine (feat/3way-merge)
# ---------------------------------------------------------------------------

SMAP = {"done": "done", "in_progress": "current sprint", "backlog": "backlog"}


def test_classify_3way_none_when_local_equals_remote():
    assert clickup.classify_3way("A", "A", "A") == "none"
    # converged: both moved to same value, base stale -> still none
    assert clickup.classify_3way("A", "B", "B") == "none"


def test_classify_3way_push_when_only_local_changed():
    assert clickup.classify_3way("A", "B", "A") == "push"


def test_classify_3way_pull_when_only_remote_changed():
    assert clickup.classify_3way("A", "A", "B") == "pull"


def test_classify_3way_conflict_when_both_changed_differently():
    assert clickup.classify_3way("A", "B", "C") == "conflict"


def test_classify_3way_missing_base_is_conflict_when_sides_differ():
    assert clickup.classify_3way(clickup._MISSING, "B", "C") == "conflict"
    # ...but agreement still wins even with no base
    assert clickup.classify_3way(clickup._MISSING, "B", "B") == "none"


def test_comparable_local_uses_status_map_and_meta_header():
    story = _story_with("T", status="in_progress", points=3,
                        milestone_label="M2", description="body")
    comp = clickup.comparable_local(story, SMAP)
    assert comp["status"] == "current sprint"          # mapped + lowercased
    assert comp["name"] == "T"
    assert comp["milestone"] is False
    assert "Points: 3" in comp["description"] and "body" in comp["description"]


def test_comparable_remote_lowercases_status():
    cu = _cu_task("x", [])
    cu["status"] = {"status": "Current Sprint"}
    cu["name"] = "R"
    assert clickup.comparable_remote(cu, SMAP)["status"] == "current sprint"
    assert clickup.comparable_remote(cu, SMAP)["name"] == "R"


def test_three_way_plan_mixed_directions():
    # base: status "backlog", name "orig"
    base = {"status": "backlog", "name": "orig", "description": "",
            "priority": None, "milestone": False}
    # local moved status -> in_progress (push); remote moved name -> "new" (pull)
    story = _story_with("orig", status="in_progress")
    cu = _cu_task("x", [])
    cu["name"] = "new"
    cu["status"] = {"status": "backlog"}
    plan = clickup.three_way_plan(base, story, cu, SMAP)
    assert plan == {"status": "push", "name": "pull"}


def test_three_way_plan_true_conflict():
    base = {"status": "backlog", "name": "orig", "description": "",
            "priority": None, "milestone": False}
    story = _story_with("local-name", status="backlog")   # local changed name
    cu = _cu_task("x", [])
    cu["name"] = "remote-name"                              # remote changed name too
    cu["status"] = {"status": "backlog"}
    plan = clickup.three_way_plan(base, story, cu, SMAP)
    assert plan == {"name": "conflict"}


def test_base_snapshot_roundtrip(tmp_path):
    data = {
        "project": {"clickup_list_id": "999", "name": "p"},
        "epics": [_epic_with("E", [
            _story_with("S1", status="in_progress", clickup_id="aaa"),
            _story_with("S2", status="backlog"),  # no clickup_id -> excluded
        ])],
    }
    p = clickup.base_snapshot_path(str(tmp_path / "proj.yaml"), "999")
    clickup.save_base_snapshot(p, data, SMAP)
    assert p.exists()
    loaded = clickup.load_base_snapshot(p)
    assert set(loaded.keys()) == {"aaa"}
    assert loaded["aaa"]["status"] == "current sprint"


def test_load_base_snapshot_absent_returns_empty(tmp_path):
    assert clickup.load_base_snapshot(tmp_path / "nope.json") == {}


def test_save_base_snapshot_records_managed_tags(tmp_path):
    # The base snapshot must persist the managed-tag universe so the next sync
    # can remove an epic/tag that was later dropped from the YAML.
    data = {
        "project": {"clickup_list_id": "999", "name": "p"},
        "epics": [_epic_with("Alpha", [
            _story_with("S1", clickup_id="aaa", tags=["stage_one"]),
        ])],
    }
    p = clickup.base_snapshot_path(str(tmp_path / "proj.yaml"), "999")
    clickup.save_base_snapshot(p, data, SMAP)
    doc = json.loads(p.read_text())
    assert "alpha" in doc["managed_tags"]        # epic name, lowercased
    assert "stage_one" in doc["managed_tags"]     # explicit tag


def test_load_base_managed_tags_roundtrip(tmp_path):
    data = {
        "project": {"clickup_list_id": "999", "name": "p"},
        "epics": [_epic_with("Beta", [
            _story_with("S1", clickup_id="aaa", tags=["stage_two"]),
        ])],
    }
    p = clickup.base_snapshot_path(str(tmp_path / "proj.yaml"), "999")
    clickup.save_base_snapshot(p, data, SMAP)
    assert clickup.load_base_managed_tags(p) >= {"beta", "stage_two"}


def test_load_base_managed_tags_absent_returns_empty(tmp_path):
    assert clickup.load_base_managed_tags(tmp_path / "nope.json") == set()


def test_load_base_managed_tags_old_base_without_key(tmp_path):
    # A pre-fix base snapshot (no managed_tags key) degrades to empty, not error.
    p = tmp_path / "base.json"
    p.write_text(json.dumps({"version": 1, "tasks": {}}))
    assert clickup.load_base_managed_tags(p) == set()


# ---------------------------------------------------------------------------
# 3-way sync integration (mocked API) — feat/3way-merge
# ---------------------------------------------------------------------------


def _3way_fixture(tmp_path, local_name, remote_name, remote_status="to do"):
    """Build a 1-story project, establish a base at name='orig'/status backlog,
    then apply a local rename and return (data, story, cu, yaml_path)."""
    smap = {"backlog": "to do", "done": "complete"}
    story = _story_with("orig", clickup_id="T1", status="backlog")
    data = {
        "project": {"name": "p", "clickup_list_id": "L1", "push_epic_tag": False},
        "status_map": smap,
        "epics": [_epic_with("E", [story])],
    }
    yaml_path = tmp_path / "p.yaml"
    with open(yaml_path, "w") as f:
        yaml.safe_dump(data, f)
    # establish base = the pre-edit agreed state
    clickup.save_base_snapshot(
        clickup.base_snapshot_path(str(yaml_path), "L1"), data, smap
    )
    story["name"] = local_name           # local edit
    cu = _cu_task("T1", [])
    cu["name"] = remote_name             # remote edit
    cu["status"] = {"status": remote_status}
    return data, story, cu, yaml_path


def _run_sync(data, cu, yaml_path, **kw):
    updates: list[dict] = []
    with mock.patch.object(clickup, "clickup_list_tasks", return_value=[cu]), \
         mock.patch.object(clickup, "clickup_get_list_members", return_value=[]), \
         mock.patch.object(clickup, "clickup_update_task",
                           side_effect=lambda t, i, b: updates.append(b) or {}), \
         mock.patch.object(clickup, "clickup_add_tag"), \
         mock.patch.object(clickup, "clickup_remove_tag"), \
         mock.patch.object(clickup, "clickup_set_custom_field"):
        stats = clickup.cmd_sync(data, str(yaml_path), **kw)
    return stats, updates


def test_sync_3way_one_sided_auto_resolve(tmp_path):
    # local changed name (->push); remote changed status to complete (->pull)
    data, story, cu, yp = _3way_fixture(tmp_path, "local-name", "orig",
                                        remote_status="complete")
    stats, updates = _run_sync(data, cu, yp, on_conflict="stop")
    assert stats["conflicts"] == 0
    assert any(u.get("name") == "local-name" for u in updates)  # name pushed
    assert story["status"] == "done"                            # status pulled


def test_sync_3way_true_conflict_stops_no_mutation(tmp_path):
    # both sides changed name -> true conflict; default stop aborts
    data, story, cu, yp = _3way_fixture(tmp_path, "local", "remote")
    stats, updates = _run_sync(data, cu, yp, on_conflict="stop")
    assert stats["conflicts"] == 1
    assert updates == []            # nothing pushed
    assert story["name"] == "local" # YAML untouched


def test_sync_3way_conflict_policy_remote_resolves(tmp_path):
    data, story, cu, yp = _3way_fixture(tmp_path, "local", "remote")
    stats, updates = _run_sync(data, cu, yp, on_conflict="remote")
    assert story["name"] == "remote"      # pulled
    assert stats["resolved_remote"] >= 1


# ---------------------------------------------------------------------------
# H1 regression: description meta-header must not double across pull->push
# ---------------------------------------------------------------------------


def test_pull_strips_meta_header_no_doubling():
    story = _story_with("T", points=3, description="hello")
    pushed = clickup.description_with_meta(story)            # "Points: 3\n\nhello"
    cu = _cu_task("x", []); cu["description"] = pushed
    clickup._pull_field_to_yaml(story, cu, "description", SMAP)
    assert story["description"] == "hello"                   # header stripped on pull
    assert clickup.description_with_meta(story) == pushed    # no doubling on re-push


def test_pull_strips_meta_header_with_remote_body_edit():
    story = _story_with("T", points=3, description="hello")
    cu = _cu_task("x", []); cu["description"] = "Points: 3\n\nhello world"  # remote edited body
    clickup._pull_field_to_yaml(story, cu, "description", SMAP)
    assert story["description"] == "hello world"
    assert clickup.description_with_meta(story) == "Points: 3\n\nhello world"


def test_pull_no_meta_passes_through():
    story = _story_with("T", points=0, description="x")      # no meta header
    cu = _cu_task("x", []); cu["description"] = "remote body"
    clickup._pull_field_to_yaml(story, cu, "description", SMAP)
    assert story["description"] == "remote body"


def test_pull_does_not_eat_user_body_that_lacks_the_header():
    story = _story_with("T", points=3, description="hello")
    cu = _cu_task("x", []); cu["description"] = "a real body with no header line"
    clickup._pull_field_to_yaml(story, cu, "description", SMAP)
    assert story["description"] == "a real body with no header line"


# ---------------------------------------------------------------------------
# M3: corrupt base must NOT silently degrade to interactive 2-way
# ---------------------------------------------------------------------------


def test_load_base_snapshot_corrupt_raises(tmp_path):
    p = tmp_path / "base.json"
    p.write_text("{not valid json")
    with pytest.raises(clickup.BaseSnapshotCorrupt):
        clickup.load_base_snapshot(p)


def test_load_base_snapshot_bad_shape_raises(tmp_path):
    p = tmp_path / "base.json"
    p.write_text('{"tasks": [1, 2, 3]}')  # tasks must be a mapping
    with pytest.raises(clickup.BaseSnapshotCorrupt):
        clickup.load_base_snapshot(p)


def test_sync_corrupt_base_refuses_no_mutation(tmp_path):
    smap = {"backlog": "to do", "done": "complete"}
    story = _story_with("orig", clickup_id="T1", status="backlog")
    data = {
        "project": {"name": "p", "clickup_list_id": "L1", "push_epic_tag": False},
        "status_map": smap,
        "epics": [_epic_with("E", [story])],
    }
    yaml_path = tmp_path / "p.yaml"
    with open(yaml_path, "w") as f:
        yaml.safe_dump(data, f)
    bp = clickup.base_snapshot_path(str(yaml_path), "L1")
    bp.parent.mkdir(parents=True, exist_ok=True)
    bp.write_text("{corrupt")  # exists but unparseable
    cu = _cu_task("T1", [])
    cu["name"] = "remote-name"
    stats, updates = _run_sync(data, cu, yaml_path, on_conflict="stop")
    assert stats["errors"] >= 1
    assert updates == []              # refused -> nothing pushed
    assert story["name"] == "orig"    # ...and nothing pulled either


# ---------------------------------------------------------------------------
# Date fields (due_date / start_date) — feat/date-fields
# ---------------------------------------------------------------------------

import datetime as _dt
from datetime import timezone as _tz


def test_yaml_date_roundtrip_noon_utc():
    ms = clickup.yaml_date_to_clickup_ms("2026-06-20")
    assert _dt.datetime.fromtimestamp(ms / 1000, _tz.utc).strftime("%Y-%m-%d %H:%M") == "2026-06-20 12:00"
    assert clickup.clickup_ms_to_yaml_date(ms) == "2026-06-20"


def test_norm_yaml_date_accepts_date_objects():
    assert clickup._norm_yaml_date(_dt.date(2026, 6, 20)) == "2026-06-20"
    assert clickup._norm_yaml_date(_dt.datetime(2026, 6, 20, 9, 30)) == "2026-06-20"
    assert clickup._norm_yaml_date("2026-06-20") == "2026-06-20"
    assert clickup._norm_yaml_date(None) is None
    assert clickup._norm_yaml_date("") is None


def test_date_conversions_none_safe():
    assert clickup.yaml_date_to_clickup_ms(None) is None
    assert clickup.clickup_ms_to_yaml_date(None) is None
    assert clickup.clickup_ms_to_yaml_date("") is None


def test_comparable_includes_dates():
    story = _story_with("T", due_date="2026-06-20")
    assert clickup.comparable_local(story, SMAP)["due_date"] == "2026-06-20"
    cu = _cu_task("x", []); cu["due_date"] = clickup.yaml_date_to_clickup_ms("2026-07-01")
    assert clickup.comparable_remote(cu, SMAP)["due_date"] == "2026-07-01"


def test_compare_task_detects_and_matches_dates():
    story = _story_with("T", clickup_id="x", status="backlog", due_date="2026-06-20")
    cu = _cu_task("x", []); cu["name"] = "T"; cu["status"] = {"status": "backlog"}; cu["due_date"] = None
    assert "due_date" in {d["field"] for d in clickup.compare_task(story, cu, SMAP)}
    cu["due_date"] = clickup.yaml_date_to_clickup_ms("2026-06-20")  # now equal at date granularity
    assert "due_date" not in {d["field"] for d in clickup.compare_task(story, cu, SMAP)}


def test_three_way_due_date_one_sided_push():
    base = {"name": "T", "status": "to do", "description": "", "priority": None,
            "milestone": False, "due_date": None, "start_date": None}
    story = _story_with("T", due_date="2026-06-20")
    cu = _cu_task("x", []); cu["name"] = "T"; cu["status"] = {"status": "to do"}
    assert clickup.three_way_plan(base, story, cu, SMAP).get("due_date") == "push"


def test_push_due_date_sends_date_time_false():
    story = _story_with("T", clickup_id="x", due_date="2026-06-20")
    cu = _cu_task("x", [])
    sent = []
    with mock.patch.object(clickup, "clickup_update_task",
                           side_effect=lambda t, i, b: sent.append(b) or {}):
        clickup._push_field_to_clickup(story, cu, "due_date", SMAP, "tok")
    assert sent and sent[0]["due_date_time"] is False
    assert sent[0]["due_date"] == clickup.yaml_date_to_clickup_ms("2026-06-20")


def test_pull_due_date_writes_date_string():
    story = _story_with("T", clickup_id="x")
    cu = _cu_task("x", []); cu["due_date"] = clickup.yaml_date_to_clickup_ms("2026-07-04")
    clickup._pull_field_to_yaml(story, cu, "due_date", SMAP)
    assert story["due_date"] == "2026-07-04"


def test_build_task_body_includes_due_date():
    story = _story_with("T", due_date="2026-06-20")
    body = clickup.build_task_body(story, SMAP)
    assert body["due_date"] == clickup.yaml_date_to_clickup_ms("2026-06-20")
    assert body["due_date_time"] is False


def test_apply_merged_value_handles_due_date():
    # H1-class gap: the LLM accept-merge path must not silently drop dates.
    story = _story_with("T", clickup_id="x", due_date="2026-06-20")
    cu = _cu_task("x", [])
    sent = []
    with mock.patch.object(clickup, "clickup_update_task",
                           side_effect=lambda t, i, b: sent.append(b) or {}):
        clickup._apply_merged_value(story, cu, "due_date", "2026-07-04", SMAP, "tok")
    assert story["due_date"] == "2026-07-04"
    assert sent and sent[0]["due_date"] == clickup.yaml_date_to_clickup_ms("2026-07-04")
    assert sent[0]["due_date_time"] is False


# ---------------------------------------------------------------------------
# 9. Dependencies (waiting_on edges): reconcile + bidirectional sync
# ---------------------------------------------------------------------------


def _cu_task_deps(task_id: str, waiting_on: list[str], extra_edges: list[dict] | None = None) -> dict:
    """A ClickUp task carrying waiting_on edges (type 1) for ``task_id``.

    ``extra_edges`` injects raw edge dicts (e.g. blocking-side mirrors where
    ``task_id`` != our id) to prove they're ignored by the reader.
    """
    base = _cu_task(task_id, [])
    edges = [
        {"task_id": task_id, "depends_on": d, "type": clickup.DEP_TYPE_WAITING_ON}
        for d in waiting_on
    ]
    if extra_edges:
        edges.extend(extra_edges)
    base["dependencies"] = edges
    return base


class TestCuWaitingOnIds:
    def test_extracts_waiting_on_for_self(self):
        cu = _cu_task_deps("T1", ["A", "B"])
        assert clickup._cu_waiting_on_ids(cu) == {"A", "B"}

    def test_ignores_blocking_side_mirror(self):
        # Edge where T1 is the depends_on (i.e. T1 blocks T9) must NOT count as
        # something T1 waits on.
        cu = _cu_task_deps(
            "T1",
            ["A"],
            extra_edges=[{"task_id": "T9", "depends_on": "T1", "type": clickup.DEP_TYPE_WAITING_ON}],
        )
        assert clickup._cu_waiting_on_ids(cu) == {"A"}

    def test_ignores_non_waiting_on_types(self):
        cu = _cu_task_deps(
            "T1",
            ["A"],
            extra_edges=[{"task_id": "T1", "depends_on": "Z", "type": 2}],  # blocking type
        )
        assert clickup._cu_waiting_on_ids(cu) == {"A"}

    def test_empty_when_no_dependencies(self):
        assert clickup._cu_waiting_on_ids(_cu_task("T1", [])) == set()


class TestResolveDependencyIds:
    def test_dedup_order_preserving(self):
        desired, unresolved = clickup._resolve_dependency_ids(["A", "B", "A"], "SELF")
        assert desired == ["A", "B"]
        assert unresolved == []

    def test_drops_blanks(self):
        desired, _ = clickup._resolve_dependency_ids(["A", "", "  ", "B"], "SELF")
        assert desired == ["A", "B"]

    def test_rejects_self_dependency(self):
        desired, unresolved = clickup._resolve_dependency_ids(["SELF", "A"], "SELF")
        assert desired == ["A"]
        assert unresolved == ["SELF"]

    def test_none_input(self):
        assert clickup._resolve_dependency_ids(None, "SELF") == ([], [])


class TestSyncDependencies:
    def test_no_key_is_noop(self):
        story = _story_with("s", clickup_id="T1")  # no depends_on key
        cu = _cu_task_deps("T1", ["A"])
        with mock.patch.object(clickup, "clickup_add_dependency") as add, \
             mock.patch.object(clickup, "clickup_remove_dependency") as rm:
            changed = clickup._sync_dependencies("tok", "T1", cu, story)
        assert changed is False
        add.assert_not_called()
        rm.assert_not_called()

    def test_adds_missing_edges(self):
        story = _story_with("s", clickup_id="T1", depends_on=["A", "B"])
        cu = _cu_task_deps("T1", ["A"])  # B is missing
        with mock.patch.object(clickup, "clickup_add_dependency") as add, \
             mock.patch.object(clickup, "clickup_remove_dependency") as rm:
            changed = clickup._sync_dependencies("tok", "T1", cu, story)
        assert changed is True
        add.assert_called_once_with("tok", "T1", "B")
        rm.assert_not_called()

    def test_empty_list_clears_all_edges(self):
        story = _story_with("s", clickup_id="T1", depends_on=[])
        cu = _cu_task_deps("T1", ["A", "B"])
        with mock.patch.object(clickup, "clickup_add_dependency") as add, \
             mock.patch.object(clickup, "clickup_remove_dependency") as rm:
            changed = clickup._sync_dependencies("tok", "T1", cu, story)
        assert changed is True
        add.assert_not_called()
        assert {c.args[2] for c in rm.call_args_list} == {"A", "B"}

    def test_mixed_add_and_remove(self):
        story = _story_with("s", clickup_id="T1", depends_on=["A", "C"])
        cu = _cu_task_deps("T1", ["A", "B"])  # add C, remove B
        with mock.patch.object(clickup, "clickup_add_dependency") as add, \
             mock.patch.object(clickup, "clickup_remove_dependency") as rm:
            clickup._sync_dependencies("tok", "T1", cu, story)
        add.assert_called_once_with("tok", "T1", "C")
        rm.assert_called_once_with("tok", "T1", "B")

    def test_already_in_sync_is_noop(self):
        story = _story_with("s", clickup_id="T1", depends_on=["A"])
        cu = _cu_task_deps("T1", ["A"])
        with mock.patch.object(clickup, "clickup_add_dependency") as add, \
             mock.patch.object(clickup, "clickup_remove_dependency") as rm:
            changed = clickup._sync_dependencies("tok", "T1", cu, story)
        assert changed is False
        add.assert_not_called()
        rm.assert_not_called()

    def test_dry_run_makes_no_calls(self):
        story = _story_with("s", clickup_id="T1", depends_on=["A", "B"])
        cu = _cu_task_deps("T1", [])
        with mock.patch.object(clickup, "clickup_add_dependency") as add, \
             mock.patch.object(clickup, "clickup_remove_dependency") as rm:
            changed = clickup._sync_dependencies("tok", "T1", cu, story, dry_run=True)
        assert changed is True
        add.assert_not_called()
        rm.assert_not_called()

    def test_self_dependency_skipped(self):
        story = _story_with("s", clickup_id="T1", depends_on=["T1", "A"])
        cu = _cu_task_deps("T1", [])
        with mock.patch.object(clickup, "clickup_add_dependency") as add, \
             mock.patch.object(clickup, "clickup_remove_dependency"):
            clickup._sync_dependencies("tok", "T1", cu, story)
        add.assert_called_once_with("tok", "T1", "A")


class TestPullDependencies:
    def test_pulls_edges_into_unmanaged_story(self):
        story = _story_with("s", clickup_id="T1")  # no depends_on key
        cu = _cu_task_deps("T1", ["B", "A"])
        assert clickup._pull_dependencies(story, cu) is True
        assert story["depends_on"] == ["A", "B"]  # sorted

    def test_no_change_when_equal(self):
        story = _story_with("s", clickup_id="T1", depends_on=["A", "B"])
        cu = _cu_task_deps("T1", ["A", "B"])
        assert clickup._pull_dependencies(story, cu) is False

    def test_remote_removal_writes_empty_list(self):
        story = _story_with("s", clickup_id="T1", depends_on=["A"])
        cu = _cu_task_deps("T1", [])  # edge removed in UI
        assert clickup._pull_dependencies(story, cu) is True
        assert story["depends_on"] == []

    def test_both_empty_no_key_added(self):
        story = _story_with("s", clickup_id="T1")  # no key
        cu = _cu_task_deps("T1", [])
        assert clickup._pull_dependencies(story, cu) is False
        assert "depends_on" not in story

    def test_clickup_task_to_story_captures_edges(self):
        cu = _cu_task_deps("T1", ["B", "A"])
        story = clickup._clickup_task_to_yaml_story(cu, SMAP)
        assert story["depends_on"] == ["A", "B"]


class TestApiHelperShapes:
    def test_add_dependency_payload(self):
        with mock.patch.object(clickup, "_api_request") as api:
            clickup.clickup_add_dependency("tok", "T1", "A")
        api.assert_called_once()
        assert api.call_args.args[0] == "POST"
        assert api.call_args.args[1].endswith("/task/T1/dependency")
        assert api.call_args.args[3] == {"depends_on": "A"}

    def test_remove_dependency_uses_query_param(self):
        with mock.patch.object(clickup, "_api_request") as api:
            clickup.clickup_remove_dependency("tok", "T1", "A")
        assert api.call_args.args[0] == "DELETE"
        assert "/task/T1/dependency?depends_on=A" in api.call_args.args[1]


class TestPushDependencyPass:
    def test_existing_task_gets_dependency_added(self, tmp_path):
        data = _data_with(
            {"VendorX Monitoring System": [
                _story_with("dev task", clickup_id="T1", depends_on=["GATE"])
            ]},
        )
        yaml_path = tmp_path / "p.yaml"
        with open(yaml_path, "w") as f:
            yaml.safe_dump(data, f)
        cu_tasks = [_cu_task_deps("T1", []), _cu_task("GATE", [])]
        with mock.patch.object(clickup, "clickup_list_tasks", return_value=cu_tasks), \
             mock.patch.object(clickup, "clickup_update_task"), \
             mock.patch.object(clickup, "clickup_add_tag"), \
             mock.patch.object(clickup, "clickup_remove_tag"), \
             mock.patch.object(clickup, "clickup_add_dependency") as add, \
             mock.patch.object(clickup, "clickup_remove_dependency"), \
             mock.patch.object(clickup, "clickup_set_custom_field"), \
             mock.patch.object(clickup, "save_yaml"):
            clickup.cmd_push(data, str(yaml_path), dry_run=False,
                             backup_path=None, backup_default=False)
        add.assert_called_once_with(clickup.get_clickup_token(), "T1", "GATE")

    def test_newly_created_task_gets_dependency_in_second_pass(self, tmp_path):
        data = _data_with(
            {"VendorX Monitoring System": [
                _story_with("new dev task", depends_on=["GATE"])  # no clickup_id
            ]},
        )
        yaml_path = tmp_path / "p.yaml"
        with open(yaml_path, "w") as f:
            yaml.safe_dump(data, f)
        with mock.patch.object(clickup, "clickup_list_tasks", return_value=[_cu_task("GATE", [])]), \
             mock.patch.object(clickup, "clickup_create_task",
                               return_value={"id": "NEW-1", "custom_id": None}), \
             mock.patch.object(clickup, "clickup_add_dependency") as add, \
             mock.patch.object(clickup, "clickup_set_custom_field"), \
             mock.patch.object(clickup, "save_yaml"):
            clickup.cmd_push(data, str(yaml_path), dry_run=False,
                             backup_path=None, backup_default=False)
        # Second pass uses the clickup_id written back from the create call.
        add.assert_called_once_with(clickup.get_clickup_token(), "NEW-1", "GATE")

    def test_dry_run_makes_no_dependency_calls(self, tmp_path):
        data = _data_with(
            {"VendorX Monitoring System": [
                _story_with("dev task", clickup_id="T1", depends_on=["GATE"])
            ]},
        )
        yaml_path = tmp_path / "p.yaml"
        with open(yaml_path, "w") as f:
            yaml.safe_dump(data, f)
        cu_tasks = [_cu_task_deps("T1", []), _cu_task("GATE", [])]
        with mock.patch.object(clickup, "clickup_list_tasks", return_value=cu_tasks), \
             mock.patch.object(clickup, "clickup_add_dependency") as add, \
             mock.patch.object(clickup, "clickup_remove_dependency") as rm, \
             mock.patch.object(clickup, "clickup_set_custom_field"):
            clickup.cmd_push(data, str(yaml_path), dry_run=True,
                             backup_path=None, backup_default=False)
        add.assert_not_called()
        rm.assert_not_called()


# ---------------------------------------------------------------------------
# 10. Markdown descriptions: preserve embedded task-mention tiles
# ---------------------------------------------------------------------------


class TestCuDescription:
    def test_prefers_markdown_over_flattened_plain(self):
        cu = {"description": "see  here", "markdown_description": "see [T](https://app.clickup.com/t/abc) here"}
        assert clickup._cu_description(cu) == "see [T](https://app.clickup.com/t/abc) here"

    def test_falls_back_to_plain_when_markdown_absent(self):
        assert clickup._cu_description({"description": "plain only"}) == "plain only"

    def test_empty_markdown_string_is_used_not_skipped(self):
        # markdown_description present but '' (board description cleared) -> use it
        assert clickup._cu_description({"description": "stale", "markdown_description": ""}) == ""

    def test_empty_task(self):
        assert clickup._cu_description({}) == ""


class TestMarkdownPushPull:
    def test_build_task_body_emits_markdown_content(self):
        story = _story_with("T", description="hi", points=3)
        body = clickup.build_task_body(story, SMAP)
        assert "markdown_content" in body
        assert "description" not in body
        assert body["markdown_content"].startswith("Points: 3")
        assert "hi" in body["markdown_content"]

    def test_no_false_diff_when_markdown_matches_yaml(self):
        # Plain field is flattened (tile -> whitespace) but markdown matches YAML.
        story = _story_with("T", clickup_id="x", description="see [T](https://app.clickup.com/t/u)")
        cu = _cu_task("x", [])
        cu["description"] = "see"  # ClickUp flattened the tile
        cu["markdown_description"] = "see [T](https://app.clickup.com/t/u)"
        diffs = clickup.compare_task(story, cu, SMAP)
        assert not any(d["field"] == "description" for d in diffs)

    def test_pull_writes_markdown_form(self):
        story = _story_with("T", clickup_id="x")
        cu = _cu_task("x", [])
        cu["markdown_description"] = "body [T](https://app.clickup.com/t/u)"
        clickup._apply_clickup_to_yaml(story, cu, SMAP)
        assert story["description"] == "body [T](https://app.clickup.com/t/u)"

    def test_list_fetch_requests_markdown(self):
        with mock.patch.object(clickup, "_api_request",
                               return_value={"tasks": [], "last_page": True}) as api:
            clickup.clickup_list_tasks("tok", "999")
        assert "include_markdown_description=true" in api.call_args.args[1]

    def test_get_task_requests_markdown(self):
        with mock.patch.object(clickup, "_api_request", return_value={}) as api:
            clickup.clickup_get_task("tok", "T1")
        assert "include_markdown_description=true" in api.call_args.args[1]

    def test_push_field_sends_markdown_content(self):
        story = _story_with("T", clickup_id="x", description="body", points=2)
        sent = []
        with mock.patch.object(clickup, "clickup_update_task",
                               side_effect=lambda t, i, b: sent.append(b) or {}):
            clickup._push_field_to_clickup(story, _cu_task("x", []), "description", SMAP, "tok")
        assert "markdown_content" in sent[0]
        assert "description" not in sent[0]


def test_sync_created_task_not_flagged_archived(tmp_path):
    """Regression: a task created during a sync run must NOT be marked
    archived_in_clickup.

    Phase-5 archive-detection flags any story whose clickup_id is absent from
    seen_cu_ids, which is seeded from the pre-run fetch. A task created mid-run
    isn't in that fetch, so without recording its new id it was false-flagged
    as archived even though it is live in ClickUp."""
    story = _story_with("brand new task")  # no clickup_id -> will be created
    data = _data_with({"Kickoff / Access": [story]})
    yaml_path = tmp_path / "p.yaml"
    with open(yaml_path, "w") as f:
        yaml.safe_dump(data, f)

    with mock.patch.object(clickup, "clickup_list_tasks", return_value=[]), \
         mock.patch.object(clickup, "clickup_get_list_members", return_value=[]), \
         mock.patch.object(clickup, "clickup_create_task",
                           return_value={"id": "NEW-1", "custom_id": None}), \
         mock.patch.object(clickup, "clickup_add_tag"), \
         mock.patch.object(clickup, "clickup_remove_tag"), \
         mock.patch.object(clickup, "clickup_set_custom_field"):
        stats = clickup.cmd_sync(data, str(yaml_path), on_conflict="stop")

    # The story was created in ClickUp...
    assert story["clickup_id"] == "NEW-1"
    assert stats["created_in_clickup"] == 1
    # ...and must NOT be false-flagged as archived.
    assert "archived_in_clickup" not in story
    assert stats["archived"] == 0


# ---------------------------------------------------------------------------
# Credential resolution: env var > pass > legacy env file
# ---------------------------------------------------------------------------


class TestPassGet:
    def _fake_run(self, returncode, stdout):
        class _R:
            pass
        r = _R()
        r.returncode = returncode
        r.stdout = stdout
        return r

    def test_returns_first_line_stripped(self, monkeypatch):
        monkeypatch.setattr(
            clickup.subprocess, "run",
            lambda *a, **k: self._fake_run(0, "pk_secret\nignored\n"),
        )
        assert clickup._pass_get("clickup/api-token") == "pk_secret"

    def test_none_on_nonzero_returncode(self, monkeypatch):
        monkeypatch.setattr(
            clickup.subprocess, "run",
            lambda *a, **k: self._fake_run(1, ""),
        )
        assert clickup._pass_get("clickup/missing") is None

    def test_none_on_empty_body(self, monkeypatch):
        monkeypatch.setattr(
            clickup.subprocess, "run",
            lambda *a, **k: self._fake_run(0, "\n"),
        )
        assert clickup._pass_get("clickup/blank") is None

    def test_none_when_pass_not_installed(self, monkeypatch):
        def boom(*a, **k):
            raise FileNotFoundError("pass")
        monkeypatch.setattr(clickup.subprocess, "run", boom)
        assert clickup._pass_get("x") is None


class TestReadEnvFileValue:
    def test_reads_export_quoted(self, tmp_path):
        f = tmp_path / "c.env"
        f.write_text('export CLICKUP_API_TOKEN="pk_file"\n')
        assert clickup._read_env_file_value(f, "CLICKUP_API_TOKEN") == "pk_file"

    def test_missing_file_is_none(self, tmp_path):
        assert clickup._read_env_file_value(tmp_path / "nope.env", "K") is None

    def test_missing_key_is_none(self, tmp_path):
        f = tmp_path / "c.env"
        f.write_text("OTHER=1\n")
        assert clickup._read_env_file_value(f, "CLICKUP_API_TOKEN") is None

    def test_does_not_mutate_environ(self, tmp_path, monkeypatch):
        monkeypatch.delenv("ZZZ_TOKEN", raising=False)
        f = tmp_path / "c.env"
        f.write_text("ZZZ_TOKEN=val\n")
        clickup._read_env_file_value(f, "ZZZ_TOKEN")
        assert "ZZZ_TOKEN" not in os.environ


class TestGetClickupToken:
    def test_env_var_wins_over_pass(self, monkeypatch):
        monkeypatch.setenv("CLICKUP_API_TOKEN", "pk_env")
        monkeypatch.delenv("CLICKUP_SANDBOX", raising=False)
        monkeypatch.setattr(clickup, "_pass_get", lambda k: "pk_pass")
        assert clickup.get_clickup_token() == "pk_env"

    def test_pass_used_when_env_absent(self, monkeypatch):
        monkeypatch.delenv("CLICKUP_API_TOKEN", raising=False)
        monkeypatch.delenv("CLICKUP_SANDBOX", raising=False)
        seen = {}
        def fake(key):
            seen["key"] = key
            return "pk_from_pass"
        monkeypatch.setattr(clickup, "_pass_get", fake)
        assert clickup.get_clickup_token() == "pk_from_pass"
        assert seen["key"] == "clickup/api-token"

    def test_sandbox_mode_selects_sandbox_pass_key(self, monkeypatch):
        monkeypatch.delenv("CLICKUP_API_TOKEN", raising=False)
        monkeypatch.delenv("CLICKUP_API_TOKEN_SANDBOX", raising=False)
        monkeypatch.setenv("CLICKUP_SANDBOX", "1")
        seen = {}
        def fake(key):
            seen["key"] = key
            return "pk_sbx"
        monkeypatch.setattr(clickup, "_pass_get", fake)
        assert clickup.get_clickup_token() == "pk_sbx"
        assert seen["key"] == "clickup/sandbox-api-token"

    def test_file_fallback_when_no_env_no_pass(self, monkeypatch, tmp_path):
        monkeypatch.delenv("CLICKUP_API_TOKEN", raising=False)
        monkeypatch.delenv("CLICKUP_SANDBOX", raising=False)
        monkeypatch.setattr(clickup, "_pass_get", lambda k: None)
        envf = tmp_path / "clickup.env"
        envf.write_text("export CLICKUP_API_TOKEN=pk_file\n")
        monkeypatch.setattr(clickup, "_prod_env_path", lambda: envf)
        assert clickup.get_clickup_token() == "pk_file"

    def test_exits_when_nothing_resolves(self, monkeypatch, tmp_path):
        monkeypatch.delenv("CLICKUP_API_TOKEN", raising=False)
        monkeypatch.delenv("CLICKUP_SANDBOX", raising=False)
        monkeypatch.setattr(clickup, "_pass_get", lambda k: None)
        monkeypatch.setattr(clickup, "_prod_env_path", lambda: tmp_path / "absent.env")
        with pytest.raises(SystemExit):
            clickup.get_clickup_token()


class TestGetOpenAIKey:
    def test_pass_key_is_clickup_openai_key(self, monkeypatch):
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        seen = {}
        def fake(key):
            seen["key"] = key
            return "sk_from_pass"
        monkeypatch.setattr(clickup, "_pass_get", fake)
        assert clickup.get_openai_key() == "sk_from_pass"
        assert seen["key"] == "clickup/openai-key"


# ---------------------------------------------------------------------------
# Relations (non-blocking "linked tasks") — mirror the dependency suite
# ---------------------------------------------------------------------------


def _cu_task_linked(task_id: str, linked: list[str], extra_links: list[dict] | None = None) -> dict:
    """A ClickUp task carrying ``linked_tasks`` entries for ``task_id``.

    Each entry models the symmetric link as ``{task_id: other, link_id: self}``.
    ``extra_links`` injects raw entries (reversed orientation, self-only,
    malformed) to prove the reader collapses to "the other end" robustly.
    """
    base = _cu_task(task_id, [])
    links = [{"task_id": other, "link_id": task_id} for other in linked]
    if extra_links:
        links.extend(extra_links)
    base["linked_tasks"] = links
    return base


class TestCuLinkedIds:
    def test_extracts_other_end(self):
        cu = _cu_task_linked("T1", ["A", "B"])
        assert clickup._cu_linked_ids(cu) == {"A", "B"}

    def test_handles_reversed_orientation(self):
        # Entry where THIS task sits in link_id and the peer in task_id.
        cu = _cu_task_linked("T1", [], extra_links=[{"task_id": "T1", "link_id": "C"}])
        assert clickup._cu_linked_ids(cu) == {"C"}

    def test_ignores_self_only_and_malformed(self):
        cu = _cu_task_linked(
            "T1", ["A"],
            extra_links=[{"task_id": "T1", "link_id": "T1"}, {}],
        )
        assert clickup._cu_linked_ids(cu) == {"A"}

    def test_empty_when_no_links(self):
        assert clickup._cu_linked_ids(_cu_task("T1", [])) == set()


class TestSyncRelations:
    def test_no_key_is_noop(self):
        story = _story_with("s", clickup_id="T1")  # no related key
        cu = _cu_task_linked("T1", ["A"])
        with mock.patch.object(clickup, "clickup_add_link") as add, \
             mock.patch.object(clickup, "clickup_remove_link") as rm:
            changed = clickup._sync_relations("tok", "T1", cu, story)
        assert changed is False
        add.assert_not_called()
        rm.assert_not_called()

    def test_adds_missing_links(self):
        story = _story_with("s", clickup_id="T1", related=["A", "B"])
        cu = _cu_task_linked("T1", ["A"])  # B missing
        with mock.patch.object(clickup, "clickup_add_link") as add, \
             mock.patch.object(clickup, "clickup_remove_link") as rm:
            changed = clickup._sync_relations("tok", "T1", cu, story)
        assert changed is True
        add.assert_called_once_with("tok", "T1", "B")
        rm.assert_not_called()

    def test_empty_list_clears_all_links(self):
        story = _story_with("s", clickup_id="T1", related=[])
        cu = _cu_task_linked("T1", ["A", "B"])
        with mock.patch.object(clickup, "clickup_add_link") as add, \
             mock.patch.object(clickup, "clickup_remove_link") as rm:
            changed = clickup._sync_relations("tok", "T1", cu, story)
        assert changed is True
        add.assert_not_called()
        assert {c.args[2] for c in rm.call_args_list} == {"A", "B"}

    def test_mixed_add_and_remove(self):
        story = _story_with("s", clickup_id="T1", related=["A", "C"])
        cu = _cu_task_linked("T1", ["A", "B"])  # add C, remove B
        with mock.patch.object(clickup, "clickup_add_link") as add, \
             mock.patch.object(clickup, "clickup_remove_link") as rm:
            clickup._sync_relations("tok", "T1", cu, story)
        add.assert_called_once_with("tok", "T1", "C")
        rm.assert_called_once_with("tok", "T1", "B")

    def test_already_in_sync_is_noop(self):
        story = _story_with("s", clickup_id="T1", related=["A"])
        cu = _cu_task_linked("T1", ["A"])
        with mock.patch.object(clickup, "clickup_add_link") as add, \
             mock.patch.object(clickup, "clickup_remove_link") as rm:
            changed = clickup._sync_relations("tok", "T1", cu, story)
        assert changed is False
        add.assert_not_called()
        rm.assert_not_called()

    def test_dry_run_makes_no_calls(self):
        story = _story_with("s", clickup_id="T1", related=["A", "B"])
        cu = _cu_task_linked("T1", [])
        with mock.patch.object(clickup, "clickup_add_link") as add, \
             mock.patch.object(clickup, "clickup_remove_link") as rm:
            changed = clickup._sync_relations("tok", "T1", cu, story, dry_run=True)
        assert changed is True
        add.assert_not_called()
        rm.assert_not_called()

    def test_self_link_skipped(self):
        story = _story_with("s", clickup_id="T1", related=["T1", "A"])
        cu = _cu_task_linked("T1", [])
        with mock.patch.object(clickup, "clickup_add_link") as add, \
             mock.patch.object(clickup, "clickup_remove_link"):
            clickup._sync_relations("tok", "T1", cu, story)
        add.assert_called_once_with("tok", "T1", "A")


class TestPullRelations:
    def test_pulls_links_into_unmanaged_story(self):
        story = _story_with("s", clickup_id="T1")  # no related key
        cu = _cu_task_linked("T1", ["B", "A"])
        assert clickup._pull_relations(story, cu) is True
        assert story["related"] == ["A", "B"]  # sorted

    def test_no_change_when_equal(self):
        story = _story_with("s", clickup_id="T1", related=["A", "B"])
        cu = _cu_task_linked("T1", ["A", "B"])
        assert clickup._pull_relations(story, cu) is False

    def test_remote_removal_writes_empty_list(self):
        story = _story_with("s", clickup_id="T1", related=["A"])
        cu = _cu_task_linked("T1", [])  # link removed in UI
        assert clickup._pull_relations(story, cu) is True
        assert story["related"] == []

    def test_both_empty_no_key_added(self):
        story = _story_with("s", clickup_id="T1")  # no key
        cu = _cu_task_linked("T1", [])
        assert clickup._pull_relations(story, cu) is False
        assert "related" not in story

    def test_clickup_task_to_story_captures_links(self):
        cu = _cu_task_linked("T1", ["B", "A"])
        story = clickup._clickup_task_to_yaml_story(cu, SMAP)
        assert story["related"] == ["A", "B"]


class TestRelationApiHelperShapes:
    def test_add_link_endpoint(self):
        with mock.patch.object(clickup, "_api_request") as api:
            clickup.clickup_add_link("tok", "T1", "A")
        api.assert_called_once()
        assert api.call_args.args[0] == "POST"
        assert api.call_args.args[1].endswith("/task/T1/link/A")

    def test_remove_link_endpoint(self):
        with mock.patch.object(clickup, "_api_request") as api:
            clickup.clickup_remove_link("tok", "T1", "A")
        assert api.call_args.args[0] == "DELETE"
        assert api.call_args.args[1].endswith("/task/T1/link/A")


class TestPushRelationPass:
    def test_existing_task_gets_relation_added(self, tmp_path):
        data = _data_with(
            {"VendorX Monitoring System": [
                _story_with("dev task", clickup_id="T1", related=["GATE"])
            ]},
        )
        yaml_path = tmp_path / "p.yaml"
        with open(yaml_path, "w") as f:
            yaml.safe_dump(data, f)
        cu_tasks = [_cu_task_linked("T1", []), _cu_task("GATE", [])]
        with mock.patch.object(clickup, "clickup_list_tasks", return_value=cu_tasks), \
             mock.patch.object(clickup, "clickup_update_task"), \
             mock.patch.object(clickup, "clickup_add_tag"), \
             mock.patch.object(clickup, "clickup_remove_tag"), \
             mock.patch.object(clickup, "clickup_add_link") as add, \
             mock.patch.object(clickup, "clickup_remove_link"), \
             mock.patch.object(clickup, "clickup_set_custom_field"), \
             mock.patch.object(clickup, "save_yaml"):
            clickup.cmd_push(data, str(yaml_path), dry_run=False,
                             backup_path=None, backup_default=False)
        add.assert_called_once_with(clickup.get_clickup_token(), "T1", "GATE")

    def test_newly_created_task_gets_relation_in_second_pass(self, tmp_path):
        data = _data_with(
            {"VendorX Monitoring System": [
                _story_with("new dev task", related=["GATE"])  # no clickup_id
            ]},
        )
        yaml_path = tmp_path / "p.yaml"
        with open(yaml_path, "w") as f:
            yaml.safe_dump(data, f)
        with mock.patch.object(clickup, "clickup_list_tasks", return_value=[_cu_task("GATE", [])]), \
             mock.patch.object(clickup, "clickup_create_task",
                               return_value={"id": "NEW-1", "custom_id": None}), \
             mock.patch.object(clickup, "clickup_add_link") as add, \
             mock.patch.object(clickup, "clickup_set_custom_field"), \
             mock.patch.object(clickup, "save_yaml"):
            clickup.cmd_push(data, str(yaml_path), dry_run=False,
                             backup_path=None, backup_default=False)
        add.assert_called_once_with(clickup.get_clickup_token(), "NEW-1", "GATE")

    def test_dry_run_makes_no_relation_calls(self, tmp_path):
        data = _data_with(
            {"VendorX Monitoring System": [
                _story_with("dev task", clickup_id="T1", related=["GATE"])
            ]},
        )
        yaml_path = tmp_path / "p.yaml"
        with open(yaml_path, "w") as f:
            yaml.safe_dump(data, f)
        cu_tasks = [_cu_task_linked("T1", []), _cu_task("GATE", [])]
        with mock.patch.object(clickup, "clickup_list_tasks", return_value=cu_tasks), \
             mock.patch.object(clickup, "clickup_add_link") as add, \
             mock.patch.object(clickup, "clickup_remove_link") as rm, \
             mock.patch.object(clickup, "clickup_set_custom_field"):
            clickup.cmd_push(data, str(yaml_path), dry_run=True,
                             backup_path=None, backup_default=False)
        add.assert_not_called()
        rm.assert_not_called()


# ---------------------------------------------------------------------------
# Sync applies link edges (roadmap #2): the relationship-reconcile second pass
# now runs inside cmd_sync, so `sync` covers depends_on + related.
# ---------------------------------------------------------------------------


def _cu_task_with_edges(task_id: str, name: str) -> dict:
    """A ClickUp task whose scalar fields match a _story_with(name) story, so
    compare_task yields no diffs and the test isolates the edge second pass."""
    cu = _cu_task(task_id, [])
    cu["name"] = name
    cu["dependencies"] = []
    cu["linked_tasks"] = []
    return cu


class TestSyncReconcilesEdges:
    def _run(self, data, yaml_path, cu_tasks, dry_run):
        with mock.patch.object(clickup, "clickup_list_tasks", return_value=cu_tasks), \
             mock.patch.object(clickup, "clickup_get_list_members", return_value=[]), \
             mock.patch.object(clickup, "clickup_update_task"), \
             mock.patch.object(clickup, "clickup_add_tag"), \
             mock.patch.object(clickup, "clickup_remove_tag"), \
             mock.patch.object(clickup, "clickup_set_custom_field"), \
             mock.patch.object(clickup, "clickup_add_dependency") as add_dep, \
             mock.patch.object(clickup, "clickup_remove_dependency"), \
             mock.patch.object(clickup, "clickup_add_link") as add_link, \
             mock.patch.object(clickup, "clickup_remove_link"), \
             mock.patch.object(clickup, "save_yaml"):
            clickup.cmd_sync(
                data, str(yaml_path), conflict="local",
                on_conflict="local", dry_run=dry_run,
            )
        return add_dep, add_link

    def test_sync_applies_dependency_and_relation(self, tmp_path):
        data = _data_with({"VendorX Monitoring System": [
            _story_with("dev task", clickup_id="T1",
                        depends_on=["GATE"], related=["REL"])
        ]})
        yaml_path = tmp_path / "p.yaml"
        with open(yaml_path, "w") as f:
            yaml.safe_dump(data, f)
        cu_tasks = [
            _cu_task_with_edges("T1", "dev task"),
            _cu_task("GATE", []), _cu_task("REL", []),
        ]
        add_dep, add_link = self._run(data, yaml_path, cu_tasks, dry_run=False)
        add_dep.assert_called_once_with(clickup.get_clickup_token(), "T1", "GATE")
        add_link.assert_called_once_with(clickup.get_clickup_token(), "T1", "REL")

    def test_sync_dry_run_makes_no_edge_calls(self, tmp_path):
        data = _data_with({"VendorX Monitoring System": [
            _story_with("dev task", clickup_id="T1",
                        depends_on=["GATE"], related=["REL"])
        ]})
        yaml_path = tmp_path / "p.yaml"
        with open(yaml_path, "w") as f:
            yaml.safe_dump(data, f)
        cu_tasks = [
            _cu_task_with_edges("T1", "dev task"),
            _cu_task("GATE", []), _cu_task("REL", []),
        ]
        add_dep, add_link = self._run(data, yaml_path, cu_tasks, dry_run=True)
        add_dep.assert_not_called()
        add_link.assert_not_called()


# ---------------------------------------------------------------------------
# Coverage gaps surfaced in review (M3): sandbox precedence, timeout, encoding,
# diff related-mismatch display.
# ---------------------------------------------------------------------------


class TestCredentialGaps:
    def test_sandbox_env_var_wins_over_pass(self, monkeypatch):
        monkeypatch.setenv("CLICKUP_SANDBOX", "1")
        monkeypatch.setenv("CLICKUP_API_TOKEN_SANDBOX", "pk_env_sbx")
        monkeypatch.setattr(clickup, "_pass_get", lambda k: "pk_pass_sbx")
        assert clickup.get_clickup_token() == "pk_env_sbx"

    def test_sandbox_file_fallback(self, monkeypatch, tmp_path):
        monkeypatch.setenv("CLICKUP_SANDBOX", "1")
        monkeypatch.delenv("CLICKUP_API_TOKEN_SANDBOX", raising=False)
        monkeypatch.setattr(clickup, "_pass_get", lambda k: None)
        f = tmp_path / "clickup-sandbox.env"
        f.write_text("export CLICKUP_API_TOKEN_SANDBOX=pk_sbx_file\n")
        monkeypatch.setattr(clickup, "_sandbox_env_path", lambda: f)
        assert clickup.get_clickup_token() == "pk_sbx_file"

    def test_pass_timeout_falls_through(self, monkeypatch):
        def timeout(*a, **k):
            raise clickup.subprocess.TimeoutExpired(cmd="pass", timeout=15)
        monkeypatch.setattr(clickup.subprocess, "run", timeout)
        assert clickup._pass_get("clickup/api-token") is None


class TestLinkUrlEncoding:
    def test_add_link_url_encodes_target(self):
        with mock.patch.object(clickup, "_api_request") as api:
            clickup.clickup_add_link("tok", "T1", "a b")
        # Space -> %20 proves urllib.parse.quote runs (default safe='/', matching
        # the existing dependency helpers). ClickUp ids never contain spaces.
        assert api.call_args.args[1].endswith("/task/T1/link/a%20b")


class TestDiffSurfacesRelation:
    def test_related_divergence_counts_as_mismatch(self, tmp_path):
        data = _data_with({"VendorX Monitoring System": [
            _story_with("dev task", clickup_id="T1", related=["X"])
        ]})
        cu_tasks = [_cu_task_with_edges("T1", "dev task")]  # no linked_tasks
        # Isolate the related path: no scalar diffs, no assignee/dep diffs.
        with mock.patch.object(clickup, "clickup_list_tasks", return_value=cu_tasks), \
             mock.patch.object(clickup, "clickup_get_list_members", return_value=[]), \
             mock.patch.object(clickup, "compare_task", return_value=[]):
            stats = clickup.cmd_diff(data)
        assert stats["mismatches"] == 1


# ---------------------------------------------------------------------------
# Edge-removal warning (H1/M1 mitigation): sync/merge warn before deleting a
# remote edge; push stays silent (authoritative overwrite is its contract).
# ---------------------------------------------------------------------------


class TestEdgeRemovalWarning:
    def test_relation_removal_warns_when_flag_set(self, caplog):
        story = _story_with("s", clickup_id="T1", related=[])  # clear -> remove A
        cu = _cu_task_linked("T1", ["A"])
        with mock.patch.object(clickup, "clickup_remove_link"), \
             caplog.at_level("WARNING"):
            clickup._sync_relations("tok", "T1", cu, story, warn_on_remove=True)
        assert any("relation edge" in r.message and "will be deleted" in r.message
                   for r in caplog.records)

    def test_relation_removal_silent_when_flag_unset(self, caplog):
        story = _story_with("s", clickup_id="T1", related=[])
        cu = _cu_task_linked("T1", ["A"])
        with mock.patch.object(clickup, "clickup_remove_link"), \
             caplog.at_level("WARNING"):
            clickup._sync_relations("tok", "T1", cu, story, warn_on_remove=False)
        assert not any("will be deleted" in r.message for r in caplog.records)

    def test_dependency_removal_warns_when_flag_set(self, caplog):
        story = _story_with("s", clickup_id="T1", depends_on=[])
        cu = _cu_task_deps("T1", ["A"])
        with mock.patch.object(clickup, "clickup_remove_dependency"), \
             caplog.at_level("WARNING"):
            clickup._sync_dependencies("tok", "T1", cu, story, warn_on_remove=True)
        assert any("dependency edge" in r.message and "will be deleted" in r.message
                   for r in caplog.records)

    def test_pure_add_does_not_warn(self, caplog):
        story = _story_with("s", clickup_id="T1", related=["A"])  # add only
        cu = _cu_task_linked("T1", [])
        with mock.patch.object(clickup, "clickup_add_link"), \
             caplog.at_level("WARNING"):
            clickup._sync_relations("tok", "T1", cu, story, warn_on_remove=True)
        assert not any("will be deleted" in r.message for r in caplog.records)

    def test_dry_run_removal_warns_with_dry_run_prefix(self, caplog):
        story = _story_with("s", clickup_id="T1", related=[])
        cu = _cu_task_linked("T1", ["A"])
        with caplog.at_level("WARNING"):
            clickup._sync_relations("tok", "T1", cu, story,
                                    dry_run=True, warn_on_remove=True)
        assert any("Would remove" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Peer-declared symmetric links (H1-footgun fix): a link declared by EITHER
# endpoint survives; removal fires only when NEITHER managed side declares it.
# ---------------------------------------------------------------------------


class TestBuildDeclaredRelations:
    def test_maps_managed_stories_only(self):
        data = _data_with({"Infrastructure + CRM": [
            _story_with("a", clickup_id="T1", related=["T2", "T2", ""]),  # dedup + blank
            _story_with("b", clickup_id="T2"),                            # no related key -> unmanaged
            _story_with("c", related=["T9"]),                             # no clickup_id -> skipped
            _story_with("d", clickup_id="T3", related=["T3", "T4"]),      # self-link dropped
        ]})
        assert clickup._build_declared_relations(data) == {"T1": {"T2"}, "T3": {"T4"}}


class TestPeerDeclaredRelationPreserved:
    def test_managed_peer_declaration_prevents_removal(self):
        # T2 is managed (related=[]) and linked to T1 in ClickUp, but the peer T1
        # declares the reciprocal. Union semantics keep the edge; nothing removed.
        story = _story_with("b", clickup_id="T2", related=[])
        cu = _cu_task_linked("T2", ["T1"])
        peer = {"T1": {"T2"}, "T2": set()}
        with mock.patch.object(clickup, "clickup_remove_link") as rm:
            changed = clickup._sync_relations("tok", "T2", cu, story, peer_declared=peer)
        rm.assert_not_called()
        assert changed is False

    def test_removal_still_happens_when_no_peer_declares(self):
        # No managed peer declares the T2<->X edge, so it is genuinely removed.
        story = _story_with("b", clickup_id="T2", related=[])
        cu = _cu_task_linked("T2", ["X"])
        peer = {"T2": set()}
        with mock.patch.object(clickup, "clickup_remove_link") as rm:
            changed = clickup._sync_relations("tok", "T2", cu, story, peer_declared=peer)
        rm.assert_called_once_with("tok", "T2", "X")
        assert changed is True

    def test_none_peer_declared_preserves_legacy_clear(self):
        # Back-compat: without the map (push / bare call), related=[] still clears.
        story = _story_with("b", clickup_id="T2", related=[])
        cu = _cu_task_linked("T2", ["T1"])
        with mock.patch.object(clickup, "clickup_remove_link") as rm:
            clickup._sync_relations("tok", "T2", cu, story)
        rm.assert_called_once_with("tok", "T2", "T1")

    def test_reconcile_pass_preserves_one_sided_link(self):
        # End-to-end via _reconcile_edges_pass: A declares related=[T2]; B(T2) is
        # managed but declares related=[] — the mutual link must NOT be removed.
        data = _data_with({"Infrastructure + CRM": [
            _story_with("a", clickup_id="T1", related=["T2"]),
            _story_with("b", clickup_id="T2", related=[]),
        ]})
        cu_by_id = {
            "T1": _cu_task_linked("T1", ["T2"]),
            "T2": _cu_task_linked("T2", ["T1"]),
        }
        stats = {"errors": 0}
        with mock.patch.object(clickup, "clickup_remove_link") as rm, \
             mock.patch.object(clickup, "clickup_add_link") as add:
            clickup._reconcile_edges_pass("tok", data, cu_by_id, stats, dry_run=False)
        rm.assert_not_called()
        add.assert_not_called()
        assert stats["errors"] == 0


# ---------------------------------------------------------------------------
# push/pull safety: one-directional warning banner + pull YAML backup.
# ---------------------------------------------------------------------------


class TestOneDirectionalWarning:
    def test_push_warns_about_clobbering_clickup(self, tmp_path, caplog):
        data = _data_with({"Kickoff / Access": [
            _story_with("t", clickup_id="T1")
        ]})
        yaml_path = tmp_path / "p.yaml"
        with open(yaml_path, "w") as f:
            yaml.safe_dump(data, f)
        cu = _cu_task_with_edges("T1", "t")
        with mock.patch.object(clickup, "clickup_list_tasks", return_value=[cu]), \
             mock.patch.object(clickup, "clickup_get_list_members", return_value=[]), \
             mock.patch.object(clickup, "_maybe_write_backup"), \
             mock.patch.object(clickup, "save_yaml"), \
             caplog.at_level("WARNING"):
            clickup.cmd_push(data, str(yaml_path), dry_run=True,
                             backup_path=None, backup_default=False)
        joined = " ".join(r.message for r in caplog.records)
        assert "`push` is a one-directional overwrite" in joined
        assert "use `sync`" in joined

    def test_pull_warns_about_clobbering_yaml(self, tmp_path, caplog):
        data = _data_with({"Kickoff / Access": [_story_with("t", clickup_id="T1")]})
        yaml_path = tmp_path / "p.yaml"
        with open(yaml_path, "w") as f:
            yaml.safe_dump(data, f)
        cu = _cu_task_with_edges("T1", "t")
        with mock.patch.object(clickup, "clickup_list_tasks", return_value=[cu]), \
             mock.patch.object(clickup, "save_yaml"), \
             caplog.at_level("WARNING"):
            clickup.cmd_pull(data, str(yaml_path), dry_run=True)
        joined = " ".join(r.message for r in caplog.records)
        assert "`pull` is a one-directional overwrite" in joined


class TestPullBackup:
    def test_pull_backs_up_yaml_by_default(self, tmp_path):
        data = _data_with({"Kickoff / Access": [_story_with("t", clickup_id="T1")]})
        yaml_path = tmp_path / "p.yaml"
        with open(yaml_path, "w") as f:
            yaml.safe_dump(data, f)
        cu = _cu_task_with_edges("T1", "t")
        with mock.patch.object(clickup, "clickup_list_tasks", return_value=[cu]), \
             mock.patch.object(clickup, "save_yaml"), \
             mock.patch.object(clickup, "_backup_yaml_file") as bk:
            clickup.cmd_pull(data, str(yaml_path), dry_run=False, backup_default=True)
        bk.assert_called_once_with(str(yaml_path), dest=None)

    def test_pull_skips_backup_when_disabled(self, tmp_path):
        data = _data_with({"Kickoff / Access": [_story_with("t", clickup_id="T1")]})
        yaml_path = tmp_path / "p.yaml"
        with open(yaml_path, "w") as f:
            yaml.safe_dump(data, f)
        cu = _cu_task_with_edges("T1", "t")
        with mock.patch.object(clickup, "clickup_list_tasks", return_value=[cu]), \
             mock.patch.object(clickup, "save_yaml"), \
             mock.patch.object(clickup, "_backup_yaml_file") as bk:
            clickup.cmd_pull(data, str(yaml_path), dry_run=False, backup_default=False)
        bk.assert_not_called()

    def test_pull_dry_run_does_not_back_up(self, tmp_path):
        data = _data_with({"Kickoff / Access": [_story_with("t", clickup_id="T1")]})
        yaml_path = tmp_path / "p.yaml"
        with open(yaml_path, "w") as f:
            yaml.safe_dump(data, f)
        cu = _cu_task_with_edges("T1", "t")
        with mock.patch.object(clickup, "clickup_list_tasks", return_value=[cu]), \
             mock.patch.object(clickup, "save_yaml"), \
             mock.patch.object(clickup, "_backup_yaml_file") as bk:
            clickup.cmd_pull(data, str(yaml_path), dry_run=True, backup_default=True)
        bk.assert_not_called()

    def test_backup_yaml_file_writes_beside_source_in_sidecar(self, tmp_path):
        # Co-located in the project-local .clickup-sync/ sidecar (NOT ~/tmp),
        # so identically-named files in different dirs never collide.
        src = tmp_path / "proj.yaml"
        src.write_text("project: {}\n")
        dst = clickup._backup_yaml_file(str(src))
        assert dst is not None and dst.exists()
        assert dst.read_text() == "project: {}\n"
        assert dst.parent == src.resolve().parent / ".clickup-sync"
        assert dst.name.startswith("yaml-backup-proj-")

    def test_backup_yaml_file_honors_explicit_dest(self, tmp_path):
        src = tmp_path / "proj.yaml"
        src.write_text("x: 1\n")
        dest = tmp_path / "custom" / "mybak.yaml"
        dst = clickup._backup_yaml_file(str(src), dest=str(dest))
        assert dst == dest and dest.read_text() == "x: 1\n"

    def test_backup_yaml_file_none_when_missing(self, tmp_path):
        assert clickup._backup_yaml_file(str(tmp_path / "nope.yaml")) is None


class TestBannerHonesty:
    def _pull(self, tmp_path, caplog, **kw):
        data = _data_with({"Kickoff / Access": [_story_with("t", clickup_id="T1")]})
        yaml_path = tmp_path / "p.yaml"
        with open(yaml_path, "w") as f:
            yaml.safe_dump(data, f)
        cu = _cu_task_with_edges("T1", "t")
        with mock.patch.object(clickup, "clickup_list_tasks", return_value=[cu]), \
             mock.patch.object(clickup, "save_yaml"), \
             mock.patch.object(clickup, "_backup_yaml_file"), \
             caplog.at_level("WARNING"):
            clickup.cmd_pull(data, str(yaml_path), **kw)
        return " ".join(r.message for r in caplog.records)

    def test_no_backup_banner_does_not_promise_a_backup(self, tmp_path, caplog):
        msg = self._pull(tmp_path, caplog, dry_run=False, backup_default=False)
        assert "NO backup will be written" in msg
        assert "backup of your YAML file is written first" not in msg

    def test_default_banner_promises_a_backup(self, tmp_path, caplog):
        msg = self._pull(tmp_path, caplog, dry_run=False, backup_default=True)
        assert "A backup of your YAML file" in msg
        assert "NO backup will be written" not in msg


class TestPullBackupFailClosed:
    def test_backup_oserror_aborts_pull(self, tmp_path):
        data = _data_with({"Kickoff / Access": [_story_with("t", clickup_id="T1")]})
        yaml_path = tmp_path / "p.yaml"
        with open(yaml_path, "w") as f:
            yaml.safe_dump(data, f)
        # If the safety copy fails, pull must abort BEFORE fetching/overwriting.
        with mock.patch.object(clickup, "_backup_yaml_file",
                               side_effect=OSError("disk full")), \
             mock.patch.object(clickup, "clickup_list_tasks") as lst, \
             mock.patch.object(clickup, "save_yaml") as save, \
             pytest.raises(SystemExit):
            clickup.cmd_pull(data, str(yaml_path), dry_run=False, backup_default=True)
        lst.assert_not_called()
        save.assert_not_called()

    def test_pull_passes_explicit_backup_path(self, tmp_path):
        data = _data_with({"Kickoff / Access": [_story_with("t", clickup_id="T1")]})
        yaml_path = tmp_path / "p.yaml"
        with open(yaml_path, "w") as f:
            yaml.safe_dump(data, f)
        cu = _cu_task_with_edges("T1", "t")
        with mock.patch.object(clickup, "clickup_list_tasks", return_value=[cu]), \
             mock.patch.object(clickup, "save_yaml"), \
             mock.patch.object(clickup, "_backup_yaml_file") as bk:
            clickup.cmd_pull(data, str(yaml_path), dry_run=False,
                             backup_default=True, backup_path="/tmp/x.yaml")
        bk.assert_called_once_with(str(yaml_path), dest="/tmp/x.yaml")


# ---------------------------------------------------------------------------
# BUG #14: interrupted + retried sync must not create duplicates
# ---------------------------------------------------------------------------


def _new_story_data(stories_per_epic, project_extra=None):
    """Like _data_with but for create-path tests: no clickup_id on stories."""
    return _data_with(stories_per_epic, project_extra=project_extra)


class TestSyncCreateCrashSafety:
    """Each new clickup_id must be flushed to the YAML file IMMEDIATELY after the
    create, so a run killed mid-create is resumable without duplicating tasks."""

    def test_id_flushed_per_create_before_interruption(self, tmp_path):
        data = _new_story_data({
            "Epic One": [_story_with("Task A"), _story_with("Task B")],
        })
        yaml_path = tmp_path / "p.yaml"
        with open(yaml_path, "w") as f:
            yaml.safe_dump(data, f)

        calls = {"n": 0}

        def _fake_create(token, list_id, body):
            calls["n"] += 1
            if calls["n"] == 1:
                return {"id": "NEW-1", "custom_id": "T-1"}
            # Simulate the shell-timeout SIGTERM landing mid-create-loop: a
            # BaseException propagates past the `except Exception` handlers and
            # skips the end-of-run save_yaml entirely.
            raise KeyboardInterrupt("killed mid-run")

        # NOTE: save_yaml is intentionally NOT mocked — we assert the real file
        # on disk gained the first task's id even though the run was killed
        # before it could reach the final save.
        with mock.patch.object(clickup, "clickup_list_tasks", return_value=[]), \
             mock.patch.object(clickup, "clickup_get_list_members", return_value=[]), \
             mock.patch.object(clickup, "clickup_create_task", side_effect=_fake_create), \
             mock.patch.object(clickup, "clickup_set_custom_field"), \
             pytest.raises(KeyboardInterrupt):
            clickup.cmd_sync(data, str(yaml_path), on_conflict="stop")

        on_disk = yaml.safe_load(yaml_path.read_text())
        first = on_disk["epics"][0]["stories"][0]
        assert first["clickup_id"] == "NEW-1", (
            "first task's clickup_id was not flushed to disk before the kill — "
            "a retry would re-create it (BUG #14)"
        )


class TestSyncCreateDedupe:
    """A retry after an interrupted create must adopt the orphan task already in
    ClickUp instead of (a) creating a second one or (b) re-importing it as a new
    YAML row."""

    def test_retry_adopts_orphan_no_duplicate(self, tmp_path):
        # YAML still thinks Task A was never created (writeback was lost), but
        # the prior killed run already made it in ClickUp.
        data = _new_story_data({"Epic One": [_story_with("Task A")]})
        yaml_path = tmp_path / "p.yaml"
        with open(yaml_path, "w") as f:
            yaml.safe_dump(data, f)

        orphan = _cu_task("ORPH-1", ["Epic One"])
        orphan["name"] = "Task A"

        with mock.patch.object(clickup, "clickup_list_tasks", return_value=[orphan]), \
             mock.patch.object(clickup, "clickup_get_list_members", return_value=[]), \
             mock.patch.object(clickup, "clickup_create_task") as create, \
             mock.patch.object(clickup, "clickup_set_custom_field"), \
             mock.patch.object(clickup, "clickup_add_tag"), \
             mock.patch.object(clickup, "clickup_remove_tag"), \
             mock.patch.object(clickup, "save_yaml"):
            stats = clickup.cmd_sync(data, str(yaml_path), on_conflict="stop")

        create.assert_not_called()  # must NOT create a duplicate
        stories = data["epics"][0]["stories"]
        assert len(stories) == 1, "orphan was re-imported as a duplicate YAML row (BUG #14)"
        assert stories[0]["clickup_id"] == "ORPH-1", "did not adopt the existing ClickUp task"
        assert stats["created_in_clickup"] == 0


# ---------------------------------------------------------------------------
# BUG #13: Epic dropdown must be a no-op when the current value already matches,
# including ClickUp's real GET shape (value = orderindex int, not the option id)
# ---------------------------------------------------------------------------


class TestEpicDropdownOrderindexNoop:
    PROJECT_CFG = {
        "epic_dropdown_field_id": "FIELD-UUID",
        "epic_dropdown_options": {
            "Kickoff / Access": "OPT-KICKOFF",
            "VendorX Monitoring System": "OPT-VENDORX",
        },
    }

    @staticmethod
    def _cu_task_dropdown_orderindex(value):
        """ClickUp GET returns a drop_down value as the option's orderindex int,
        with the id<->orderindex mapping carried in type_config.options."""
        cu = _cu_task("T1", [])
        cu["custom_fields"] = [{
            "id": "FIELD-UUID",
            "type": "drop_down",
            "value": value,
            "type_config": {"options": [
                {"id": "OPT-KICKOFF", "name": "Kickoff / Access", "orderindex": 0},
                {"id": "OPT-VENDORX", "name": "VendorX Monitoring System", "orderindex": 1},
            ]},
        }]
        return cu

    def test_noop_when_orderindex_already_matches(self):
        epic = _epic_with("VendorX Monitoring System", [])
        story = _story_with("s1")
        cu_task = self._cu_task_dropdown_orderindex(1)  # orderindex 1 == OPT-VENDORX
        with mock.patch.object(clickup, "clickup_set_custom_field") as set_cf:
            attempted = clickup._push_epic_dropdown_if_needed(
                "tok", "T1", cu_task, story, epic, project_cfg=self.PROJECT_CFG, dry_run=False
            )
        assert attempted is False, "re-PATCHed an Epic dropdown that already matched (BUG #13)"
        set_cf.assert_not_called()

    def test_writes_when_orderindex_differs(self):
        epic = _epic_with("VendorX Monitoring System", [])
        story = _story_with("s1")
        cu_task = self._cu_task_dropdown_orderindex(0)  # orderindex 0 == OPT-KICKOFF
        with mock.patch.object(clickup, "clickup_set_custom_field") as set_cf:
            attempted = clickup._push_epic_dropdown_if_needed(
                "tok", "T1", cu_task, story, epic, project_cfg=self.PROJECT_CFG, dry_run=False
            )
        assert attempted is True
        set_cf.assert_called_once_with("tok", "T1", "FIELD-UUID", "OPT-VENDORX")


# ---------------------------------------------------------------------------
# Dependency visibility: a dry-run must preview dependency edges with the same
# fidelity relations already get (add/remove preview + pre-removal warning),
# INCLUDING for a task that would be created this run, and must name the task
# it is talking about. Pins the sync path specifically — the inline scope
# comment once claimed sync did not handle dependencies, and nothing failed
# when that became untrue.
# ---------------------------------------------------------------------------


class TestDependencyVisibility:
    """Every case drives the real command (not the helper) so the whole
    second-pass wiring is under test, and asserts on emitted log lines."""

    def _sync(self, data, yaml_path, cu_tasks, caplog, dry_run, level="INFO"):
        with mock.patch.object(clickup, "clickup_list_tasks", return_value=cu_tasks), \
             mock.patch.object(clickup, "clickup_get_list_members", return_value=[]), \
             mock.patch.object(clickup, "clickup_update_task"), \
             mock.patch.object(clickup, "clickup_add_tag"), \
             mock.patch.object(clickup, "clickup_remove_tag"), \
             mock.patch.object(clickup, "clickup_set_custom_field"), \
             mock.patch.object(clickup, "clickup_create_task",
                               return_value={"id": "NEW-1", "custom_id": None}), \
             mock.patch.object(clickup, "clickup_add_dependency") as add_dep, \
             mock.patch.object(clickup, "clickup_remove_dependency") as rm_dep, \
             mock.patch.object(clickup, "clickup_add_link"), \
             mock.patch.object(clickup, "clickup_remove_link"), \
             mock.patch.object(clickup, "save_yaml"), \
             caplog.at_level(level):
            clickup.cmd_sync(
                data, str(yaml_path), conflict="local",
                on_conflict="local", dry_run=dry_run,
            )
        return add_dep, rm_dep, " | ".join(r.message for r in caplog.records)

    def _yaml(self, tmp_path, stories):
        data = _data_with({"VendorX Monitoring System": stories})
        yaml_path = tmp_path / "p.yaml"
        with open(yaml_path, "w") as f:
            yaml.safe_dump(data, f)
        return data, yaml_path

    # -- sync APPLIES a declared dependency (pins the scope claim) -----------

    def test_sync_applies_declared_dependency(self, tmp_path, caplog):
        """sync — not just push — runs the dependency second pass."""
        data, yp = self._yaml(tmp_path, [
            _story_with("dev task", clickup_id="T1", depends_on=["GATE"])
        ])
        cu = [_cu_task_with_edges("T1", "dev task"), _cu_task("GATE", [])]
        add_dep, _, _ = self._sync(data, yp, cu, caplog, dry_run=False)
        add_dep.assert_called_once_with(clickup.get_clickup_token(), "T1", "GATE")

    def test_sync_clears_dependencies_on_empty_list(self, tmp_path, caplog):
        data, yp = self._yaml(tmp_path, [
            _story_with("dev task", clickup_id="T1", depends_on=[])
        ])
        cu = [_cu_task_with_edges("T1", "dev task")]
        cu[0]["dependencies"] = [
            {"task_id": "T1", "depends_on": "GATE", "type": clickup.DEP_TYPE_WAITING_ON}
        ]
        _, rm_dep, _ = self._sync(data, yp, cu, caplog, dry_run=False)
        rm_dep.assert_called_once_with(clickup.get_clickup_token(), "T1", "GATE")

    def test_apply_logs_the_added_dependency_at_info(self, tmp_path, caplog):
        data, yp = self._yaml(tmp_path, [
            _story_with("dev task", clickup_id="T1", depends_on=["GATE"])
        ])
        cu = [_cu_task_with_edges("T1", "dev task"), _cu_task("GATE", [])]
        _, _, msgs = self._sync(data, yp, cu, caplog, dry_run=False)
        assert "Added dependency" in msgs and "GATE" in msgs
        assert "dev task" in msgs, "apply line does not name the task it changed"

    # -- dry-run PREVIEWS additions and removals ----------------------------

    def test_dry_run_previews_dependency_addition(self, tmp_path, caplog):
        data, yp = self._yaml(tmp_path, [
            _story_with("dev task", clickup_id="T1", depends_on=["GATE"])
        ])
        cu = [_cu_task_with_edges("T1", "dev task"), _cu_task("GATE", [])]
        add_dep, _, msgs = self._sync(data, yp, cu, caplog, dry_run=True)
        add_dep.assert_not_called()
        assert "dependencies" in msgs and "GATE" in msgs
        assert "dev task" in msgs, "preview does not name the task it would change"

    def test_dry_run_previews_dependency_removal(self, tmp_path, caplog):
        data, yp = self._yaml(tmp_path, [
            _story_with("dev task", clickup_id="T1", depends_on=[])
        ])
        cu = [_cu_task_with_edges("T1", "dev task")]
        cu[0]["dependencies"] = [
            {"task_id": "T1", "depends_on": "GATE", "type": clickup.DEP_TYPE_WAITING_ON}
        ]
        _, rm_dep, msgs = self._sync(data, yp, cu, caplog, dry_run=True)
        rm_dep.assert_not_called()
        assert "dependencies" in msgs and "GATE" in msgs

    def test_dry_run_warns_before_a_dependency_removal(self, tmp_path, caplog):
        """The destructive case: `depends_on: []` deletes real blockers, so the
        preview must carry the same loud warning relations already get."""
        data, yp = self._yaml(tmp_path, [
            _story_with("dev task", clickup_id="T1", depends_on=[])
        ])
        cu = [_cu_task_with_edges("T1", "dev task")]
        cu[0]["dependencies"] = [
            {"task_id": "T1", "depends_on": "GATE", "type": clickup.DEP_TYPE_WAITING_ON}
        ]
        _, _, msgs = self._sync(data, yp, cu, caplog, dry_run=True, level="WARNING")
        assert "dependency edge" in msgs and "will be deleted" in msgs
        assert "Would remove" in msgs

    # -- dry-run preview for a task that does not exist yet -----------------

    def test_dry_run_previews_edges_for_a_task_it_would_create(self, tmp_path, caplog):
        """A story with no clickup_id is created by the real run and gets its
        declared edges in the second pass. Under --dry-run no id exists, so the
        edge preview must still surface them rather than going silent."""
        data, yp = self._yaml(tmp_path, [
            _story_with("brand new", depends_on=["GATE"], related=["REL"])
        ])
        cu = [_cu_task("GATE", []), _cu_task("REL", [])]
        _, _, msgs = self._sync(data, yp, cu, caplog, dry_run=True)
        assert "GATE" in msgs, "dependency on a to-be-created task was not previewed"
        assert "REL" in msgs, "relation on a to-be-created task was not previewed"
        assert "brand new" in msgs

    # -- same target declared as both a dependency and a relation -----------

    def test_warns_when_same_target_is_both_dependency_and_relation(
        self, tmp_path, caplog
    ):
        """ClickUp permits both edges on one pair, but it is nearly always the
        same intent declared twice — say so rather than silently creating two."""
        data, yp = self._yaml(tmp_path, [
            _story_with("dev task", clickup_id="T1",
                        depends_on=["GATE"], related=["GATE"])
        ])
        cu = [_cu_task_with_edges("T1", "dev task"), _cu_task("GATE", [])]
        _, _, msgs = self._sync(data, yp, cu, caplog, dry_run=True, level="WARNING")
        assert "GATE" in msgs and "both" in msgs.lower()

    def test_no_overlap_warning_when_targets_differ(self, tmp_path, caplog):
        data, yp = self._yaml(tmp_path, [
            _story_with("dev task", clickup_id="T1",
                        depends_on=["GATE"], related=["REL"])
        ])
        cu = [_cu_task_with_edges("T1", "dev task"),
              _cu_task("GATE", []), _cu_task("REL", [])]
        _, _, msgs = self._sync(data, yp, cu, caplog, dry_run=True, level="WARNING")
        assert "both a dependency" not in msgs


# ---------------------------------------------------------------------------
# Live-captured shape regression
#
# Every other edge test in this file feeds the parsers a hand-written dict.
# That pins our ASSUMPTION about ClickUp's JSON, not ClickUp's JSON. These
# tests replay a response captured verbatim from the live sandbox
# (tools/capture_edge_fixture.py) so a shape change on ClickUp's side, or a
# parser change here, is caught by the suite.
#
# The question this answers: GET /list/{id}/task is the ONLY read behind
# push/sync/merge/pull/diff. If it omitted `dependencies`/`linked_tasks` the
# way some endpoints do, `current` would always be empty and edge REMOVAL
# would silently never happen. Confirmed 2026-07-25: it does not omit them.
# ---------------------------------------------------------------------------

LIVE_EDGES = json.loads(
    (HERE / "fixtures" / "live_list_edges.json").read_text()
)


class TestLiveListEndpointEdgeShape:
    """Pins the real GET /list/{id}/task payload for dependency + link edges."""

    def test_list_response_populates_both_edge_arrays(self):
        # The core claim. Not empty, not absent.
        for key in ("list_A", "list_B"):
            task = LIVE_EDGES[key]
            assert task["dependencies"], f"{key} lost its dependencies array"
            assert task["linked_tasks"], f"{key} lost its linked_tasks array"

    def test_dependency_edge_field_names_and_values(self):
        (edge,) = LIVE_EDGES["list_A"]["dependencies"]
        assert edge["task_id"] == LIVE_EDGES["a_id"]
        assert edge["depends_on"] == LIVE_EDGES["b_id"]
        assert edge["type"] == clickup.DEP_TYPE_WAITING_ON == 1

    def test_link_edge_field_names_and_values(self):
        (link,) = LIVE_EDGES["list_A"]["linked_tasks"]
        assert link["task_id"] == LIVE_EDGES["a_id"]
        assert link["link_id"] == LIVE_EDGES["b_id"]

    def test_parsers_read_the_live_shape(self):
        a, b = LIVE_EDGES["list_A"], LIVE_EDGES["list_B"]
        aid, bid = LIVE_EDGES["a_id"], LIVE_EDGES["b_id"]
        # A waits on B; B waits on nothing.
        assert clickup._cu_waiting_on_ids(a) == {bid}
        assert clickup._cu_waiting_on_ids(b) == set()
        # The link is symmetric: each side sees the other.
        assert clickup._cu_linked_ids(a) == {bid}
        assert clickup._cu_linked_ids(b) == {aid}

    def test_dependency_edge_is_shared_not_mirrored(self):
        # ClickUp does NOT emit a second, reversed edge on the blocked task --
        # it repeats the SAME object on both. _cu_waiting_on_ids therefore
        # cannot rely on a mirror existing; its task_id == self filter is what
        # keeps B from claiming it waits on A.
        assert LIVE_EDGES["list_A"]["dependencies"] == LIVE_EDGES["list_B"]["dependencies"]

    def test_list_endpoint_matches_single_task_endpoint(self):
        # If these ever diverge, the per-task GET hydration question reopens.
        for lst, get in (("list_A", "get_A"), ("list_B", "get_B")):
            assert LIVE_EDGES[lst]["dependencies"] == LIVE_EDGES[get]["dependencies"]
            assert LIVE_EDGES[lst]["linked_tasks"] == LIVE_EDGES[get]["linked_tasks"]


# ---------------------------------------------------------------------------
# 13. Parent (subtask hierarchy): reference by story name, reconciled as an edge
# ---------------------------------------------------------------------------


def _cu_task_parent(task_id: str, parent: str | None = None) -> dict:
    """A ClickUp task carrying a ``parent`` edge (the subtask shape)."""
    base = _cu_task(task_id, [])
    base["parent"] = parent
    base["top_level_parent"] = parent
    return base


def _parent_data(stories: list[dict]) -> dict:
    return _data_with({"Contact export": stories})


class TestResolveParentId:
    def test_resolves_by_story_name(self):
        data = _parent_data([
            _story_with("Export hub", clickup_id="HUB"),
            _story_with("Assign owners", clickup_id="T1", parent="Export hub"),
        ])
        ctx = clickup._build_parent_context(data)
        assert clickup._resolve_parent_id("Export hub", ctx, "T1") == "HUB"

    def test_name_match_is_case_insensitive_and_trimmed(self):
        data = _parent_data([
            _story_with("Export Hub", clickup_id="HUB"),
            _story_with("child", clickup_id="T1", parent="  export hub  "),
        ])
        ctx = clickup._build_parent_context(data)
        assert clickup._resolve_parent_id("  export hub  ", ctx, "T1") == "HUB"

    def test_accepts_a_raw_clickup_id_of_a_story_in_the_file(self):
        data = _parent_data([
            _story_with("Export hub", clickup_id="HUB"),
            _story_with("child", clickup_id="T1", parent="HUB"),
        ])
        ctx = clickup._build_parent_context(data)
        assert clickup._resolve_parent_id("HUB", ctx, "T1") == "HUB"

    def test_accepts_an_id_for_a_parent_outside_this_yaml(self):
        # No story by that name and no whitespace -> treated as a literal id, so
        # a hub that lives outside this file (or another list) still works.
        data = _parent_data([_story_with("child", clickup_id="T1", parent="86bb5nbqg")])
        ctx = clickup._build_parent_context(data)
        assert clickup._resolve_parent_id("86bb5nbqg", ctx, "T1") == "86bb5nbqg"

    def test_ambiguous_name_raises(self):
        data = _parent_data([
            _story_with("Export hub", clickup_id="HUB1"),
            _story_with("Export hub", clickup_id="HUB2"),
            _story_with("child", clickup_id="T1", parent="Export hub"),
        ])
        ctx = clickup._build_parent_context(data)
        with pytest.raises(ValueError, match="ambiguous"):
            clickup._resolve_parent_id("Export hub", ctx, "T1")

    def test_unknown_name_raises(self):
        data = _parent_data([_story_with("child", clickup_id="T1", parent="No such hub")])
        ctx = clickup._build_parent_context(data)
        with pytest.raises(ValueError, match="no story named"):
            clickup._resolve_parent_id("No such hub", ctx, "T1")

    def test_self_parent_by_name_raises(self):
        data = _parent_data([_story_with("child", clickup_id="T1", parent="child")])
        ctx = clickup._build_parent_context(data)
        with pytest.raises(ValueError, match="its own parent"):
            clickup._resolve_parent_id("child", ctx, "T1")

    def test_self_parent_by_id_raises(self):
        data = _parent_data([_story_with("child", clickup_id="T1", parent="T1")])
        ctx = clickup._build_parent_context(data)
        with pytest.raises(ValueError, match="its own parent"):
            clickup._resolve_parent_id("T1", ctx, "T1")

    def test_parent_without_clickup_id_raises_pending(self):
        # The reconcile pass runs after creates, so this only happens when the
        # parent's own create failed — say so rather than silently skipping.
        data = _parent_data([
            _story_with("Export hub"),  # never created
            _story_with("child", clickup_id="T1", parent="Export hub"),
        ])
        ctx = clickup._build_parent_context(data)
        with pytest.raises(ValueError, match="has no clickup_id"):
            clickup._resolve_parent_id("Export hub", ctx, "T1")


class TestSyncParent:
    def _ctx(self, stories):
        return clickup._build_parent_context(_parent_data(stories))

    def test_no_key_is_noop(self):
        story = _story_with("child", clickup_id="T1")  # no parent key
        cu = _cu_task_parent("T1", "OLD")
        ctx = self._ctx([story])
        with mock.patch.object(clickup, "clickup_update_task") as put:
            changed = clickup._sync_parent("tok", "T1", cu, story, ctx)
        assert changed is False
        put.assert_not_called()

    def test_sets_parent_when_absent_remotely(self):
        stories = [
            _story_with("Export hub", clickup_id="HUB"),
            _story_with("child", clickup_id="T1", parent="Export hub"),
        ]
        cu = _cu_task_parent("T1", None)
        with mock.patch.object(clickup, "clickup_update_task") as put:
            changed = clickup._sync_parent("tok", "T1", cu, stories[1], self._ctx(stories))
        assert changed is True
        put.assert_called_once_with("tok", "T1", {"parent": "HUB"})

    def test_already_correct_is_noop(self):
        stories = [
            _story_with("Export hub", clickup_id="HUB"),
            _story_with("child", clickup_id="T1", parent="Export hub"),
        ]
        cu = _cu_task_parent("T1", "HUB")
        with mock.patch.object(clickup, "clickup_update_task") as put:
            changed = clickup._sync_parent("tok", "T1", cu, stories[1], self._ctx(stories))
        assert changed is False
        put.assert_not_called()

    def test_reparents_when_remote_differs(self):
        stories = [
            _story_with("New hub", clickup_id="HUB2"),
            _story_with("child", clickup_id="T1", parent="New hub"),
        ]
        cu = _cu_task_parent("T1", "HUB1")
        with mock.patch.object(clickup, "clickup_update_task") as put:
            changed = clickup._sync_parent(
                "tok", "T1", cu, stories[1], self._ctx(stories), warn_on_remove=True
            )
        assert changed is True
        put.assert_called_once_with("tok", "T1", {"parent": "HUB2"})

    def test_dry_run_makes_no_calls(self):
        stories = [
            _story_with("Export hub", clickup_id="HUB"),
            _story_with("child", clickup_id="T1", parent="Export hub"),
        ]
        cu = _cu_task_parent("T1", None)
        with mock.patch.object(clickup, "clickup_update_task") as put:
            changed = clickup._sync_parent(
                "tok", "T1", cu, stories[1], self._ctx(stories), dry_run=True
            )
        assert changed is True
        put.assert_not_called()

    def test_declared_empty_with_no_remote_parent_is_noop(self):
        # `parent:` present but empty means "top level" — already true remotely.
        story = _story_with("child", clickup_id="T1", parent=None)
        cu = _cu_task_parent("T1", None)
        with mock.patch.object(clickup, "clickup_update_task") as put:
            changed = clickup._sync_parent("tok", "T1", cu, story, self._ctx([story]))
        assert changed is False
        put.assert_not_called()

    def test_declared_empty_with_remote_parent_raises(self):
        # The API cannot un-parent (PUT parent:null is a silent 200 no-op), so
        # this must fail loudly instead of diverging in silence.
        story = _story_with("child", clickup_id="T1", parent=None)
        cu = _cu_task_parent("T1", "HUB")
        with mock.patch.object(clickup, "clickup_update_task") as put:
            with pytest.raises(ValueError, match="cannot un-parent"):
                clickup._sync_parent("tok", "T1", cu, story, self._ctx([story]))
        put.assert_not_called()

    def test_unresolvable_parent_raises(self):
        story = _story_with("child", clickup_id="T1", parent="No such hub")
        cu = _cu_task_parent("T1", None)
        with pytest.raises(ValueError):
            clickup._sync_parent("tok", "T1", cu, story, self._ctx([story]))


class TestPullParent:
    def test_pulls_remote_parent_as_story_name(self):
        stories = [
            _story_with("Export hub", clickup_id="HUB"),
            _story_with("child", clickup_id="T1"),  # no parent key
        ]
        ctx = clickup._build_parent_context(_parent_data(stories))
        cu = _cu_task_parent("T1", "HUB")
        assert clickup._pull_parent(stories[1], cu, ctx) is True
        assert stories[1]["parent"] == "Export hub"

    def test_pulls_raw_id_when_parent_not_in_yaml(self):
        story = _story_with("child", clickup_id="T1")
        ctx = clickup._build_parent_context(_parent_data([story]))
        cu = _cu_task_parent("T1", "OUTSIDE")
        assert clickup._pull_parent(story, cu, ctx) is True
        assert story["parent"] == "OUTSIDE"

    def test_no_change_when_name_already_resolves_to_remote_parent(self):
        stories = [
            _story_with("Export hub", clickup_id="HUB"),
            _story_with("child", clickup_id="T1", parent="Export hub"),
        ]
        ctx = clickup._build_parent_context(_parent_data(stories))
        cu = _cu_task_parent("T1", "HUB")
        assert clickup._pull_parent(stories[1], cu, ctx) is False
        assert stories[1]["parent"] == "Export hub"  # not rewritten to the id

    def test_remote_unparented_writes_empty_parent(self):
        stories = [
            _story_with("Export hub", clickup_id="HUB"),
            _story_with("child", clickup_id="T1", parent="Export hub"),
        ]
        ctx = clickup._build_parent_context(_parent_data(stories))
        cu = _cu_task_parent("T1", None)  # promoted to top level in the UI
        assert clickup._pull_parent(stories[1], cu, ctx) is True
        assert stories[1]["parent"] is None

    def test_both_empty_adds_no_key(self):
        story = _story_with("child", clickup_id="T1")
        ctx = clickup._build_parent_context(_parent_data([story]))
        cu = _cu_task_parent("T1", None)
        assert clickup._pull_parent(story, cu, ctx) is False
        assert "parent" not in story

    def test_new_story_from_clickup_records_parent_name(self):
        stories = [_story_with("Export hub", clickup_id="HUB")]
        ctx = clickup._build_parent_context(_parent_data(stories))
        cu = _cu_task_parent("NEW", "HUB")
        story = clickup._clickup_task_to_yaml_story(cu, SMAP, ctx)
        assert story["parent"] == "Export hub"

    def test_new_story_from_clickup_without_context_records_id(self):
        cu = _cu_task_parent("NEW", "HUB")
        story = clickup._clickup_task_to_yaml_story(cu, SMAP)
        assert story["parent"] == "HUB"

    def test_top_level_story_gets_no_parent_key(self):
        story = clickup._clickup_task_to_yaml_story(_cu_task_parent("NEW", None), SMAP)
        assert "parent" not in story


class TestPushParentPass:
    def _write(self, tmp_path, data):
        p = tmp_path / "p.yaml"
        with open(p, "w") as f:
            yaml.safe_dump(data, f)
        return str(p)

    def test_existing_child_gets_parent_set(self, tmp_path):
        data = _parent_data([
            _story_with("Export hub", clickup_id="HUB"),
            _story_with("Assign owners", clickup_id="T1", parent="Export hub"),
        ])
        yaml_path = self._write(tmp_path, data)
        cu_tasks = [_cu_task_parent("HUB", None), _cu_task_parent("T1", None)]
        with mock.patch.object(clickup, "clickup_list_tasks", return_value=cu_tasks), \
             mock.patch.object(clickup, "clickup_update_task") as put, \
             mock.patch.object(clickup, "clickup_add_tag"), \
             mock.patch.object(clickup, "clickup_remove_tag"), \
             mock.patch.object(clickup, "clickup_set_custom_field"), \
             mock.patch.object(clickup, "save_yaml"):
            clickup.cmd_push(data, yaml_path, dry_run=False,
                             backup_path=None, backup_default=False)
        parent_calls = [c for c in put.call_args_list if c.args[2] == {"parent": "HUB"}]
        assert parent_calls and parent_calls[0].args[1] == "T1"

    def test_new_parent_and_child_link_in_one_run(self, tmp_path):
        # The headline case for name references: neither task has an id yet, so
        # an id-only `parent:` would need two syncs.
        data = _parent_data([
            _story_with("Export hub"),
            _story_with("Assign owners", parent="Export hub"),
        ])
        yaml_path = self._write(tmp_path, data)
        created = iter([{"id": "HUB-NEW", "custom_id": None},
                        {"id": "CHILD-NEW", "custom_id": None}])
        with mock.patch.object(clickup, "clickup_list_tasks", return_value=[]), \
             mock.patch.object(clickup, "clickup_create_task",
                               side_effect=lambda *a, **k: next(created)), \
             mock.patch.object(clickup, "clickup_update_task") as put, \
             mock.patch.object(clickup, "clickup_set_custom_field"), \
             mock.patch.object(clickup, "save_yaml"):
            clickup.cmd_push(data, yaml_path, dry_run=False,
                             backup_path=None, backup_default=False)
        assert [(c.args[1], c.args[2]) for c in put.call_args_list
                if "parent" in c.args[2]] == [("CHILD-NEW", {"parent": "HUB-NEW"})]

    def test_dry_run_makes_no_parent_call(self, tmp_path):
        data = _parent_data([
            _story_with("Export hub", clickup_id="HUB"),
            _story_with("Assign owners", clickup_id="T1", parent="Export hub"),
        ])
        yaml_path = self._write(tmp_path, data)
        cu_tasks = [_cu_task_parent("HUB", None), _cu_task_parent("T1", None)]
        with mock.patch.object(clickup, "clickup_list_tasks", return_value=cu_tasks), \
             mock.patch.object(clickup, "clickup_update_task") as put, \
             mock.patch.object(clickup, "clickup_set_custom_field"):
            clickup.cmd_push(data, yaml_path, dry_run=True,
                             backup_path=None, backup_default=False)
        assert not [c for c in put.call_args_list if "parent" in c.args[2]]

    def test_unresolvable_parent_counts_an_error_without_crashing(self, tmp_path):
        data = _parent_data([
            _story_with("Assign owners", clickup_id="T1", parent="No such hub"),
        ])
        yaml_path = self._write(tmp_path, data)
        with mock.patch.object(clickup, "clickup_list_tasks",
                               return_value=[_cu_task_parent("T1", None)]), \
             mock.patch.object(clickup, "clickup_update_task"), \
             mock.patch.object(clickup, "clickup_add_tag"), \
             mock.patch.object(clickup, "clickup_remove_tag"), \
             mock.patch.object(clickup, "clickup_set_custom_field"), \
             mock.patch.object(clickup, "save_yaml"):
            stats = clickup.cmd_push(data, yaml_path, dry_run=False,
                                     backup_path=None, backup_default=False)
        assert stats["errors"] >= 1

    def test_dry_run_previews_parent_for_a_pending_create(self, tmp_path, caplog):
        data = _parent_data([
            _story_with("Export hub", clickup_id="HUB"),
            _story_with("Assign owners", parent="Export hub"),  # would be created
        ])
        yaml_path = self._write(tmp_path, data)
        with mock.patch.object(clickup, "clickup_list_tasks",
                               return_value=[_cu_task_parent("HUB", None)]), \
             mock.patch.object(clickup, "clickup_set_custom_field"), \
             caplog.at_level("INFO"):
            clickup.cmd_push(data, yaml_path, dry_run=True,
                             backup_path=None, backup_default=False)
        assert "parent" in caplog.text.lower()


class TestDiffParent:
    def test_diff_reports_a_parent_mismatch(self, tmp_path, caplog):
        data = _parent_data([
            _story_with("Export hub", clickup_id="HUB"),
            _story_with("Assign owners", clickup_id="T1", parent="Export hub"),
        ])
        cu_tasks = [_cu_task_parent("HUB", None), _cu_task_parent("T1", None)]
        with mock.patch.object(clickup, "clickup_list_tasks", return_value=cu_tasks), \
             mock.patch.object(clickup, "clickup_get_list_members", return_value=[]), \
             caplog.at_level("INFO"):
            clickup.cmd_diff(data)
        assert "parent" in caplog.text.lower()


class TestParentRoundTripSafety:
    """A pulled `parent` must be a value push can resolve again."""

    def test_pull_writes_id_not_an_ambiguous_name(self):
        # Two stories share the parent's name, so the NAME form would be refused
        # by _resolve_parent_id on the next push — write the id instead.
        stories = [
            _story_with("Export hub", clickup_id="HUB"),
            _story_with("Export hub", clickup_id="HUB2"),
            _story_with("child", clickup_id="T1"),
        ]
        ctx = clickup._build_parent_context(_parent_data(stories))
        cu = _cu_task_parent("T1", "HUB")
        assert clickup._pull_parent(stories[2], cu, ctx) is True
        assert stories[2]["parent"] == "HUB"
        # And the value written round-trips.
        assert clickup._resolve_parent_id(stories[2]["parent"], ctx, "T1") == "HUB"

    def test_import_writes_id_not_an_ambiguous_name(self):
        stories = [
            _story_with("Export hub", clickup_id="HUB"),
            _story_with("Export hub", clickup_id="HUB2"),
        ]
        ctx = clickup._build_parent_context(_parent_data(stories))
        story = clickup._clickup_task_to_yaml_story(_cu_task_parent("NEW", "HUB"), SMAP, ctx)
        assert story["parent"] == "HUB"

    def test_pulled_name_survives_a_push_resolution(self):
        stories = [
            _story_with("Export hub", clickup_id="HUB"),
            _story_with("child", clickup_id="T1"),
        ]
        ctx = clickup._build_parent_context(_parent_data(stories))
        clickup._pull_parent(stories[1], _cu_task_parent("T1", "HUB"), ctx)
        assert clickup._resolve_parent_id(stories[1]["parent"], ctx, "T1") == "HUB"


class TestParentSameListGuard:
    """A cross-list parent is refused: ClickUp's PUT would MOVE the task.

    Verified live — `PUT /task/{id} {"parent": <task in another list>}` returns
    200 and relocates the task into the parent's list, where the next sync sees
    it as archived. (The create endpoint refuses the same thing outright with
    ITEM_137 "Parent not child of list".) So the tool checks membership first.
    """

    def _ctx(self, stories, cu_by_id=None):
        return clickup._build_parent_context(
            _parent_data(stories), cu_by_id=cu_by_id or {}
        )

    def test_refuses_a_parent_id_in_another_list(self):
        story = _story_with("child", clickup_id="T1", parent="OUTSIDE")
        ctx = self._ctx([story])
        elsewhere = _cu_task_parent("OUTSIDE", None)
        elsewhere["list"] = {"id": "111111"}  # not the project's list (999999)
        with mock.patch.object(clickup, "clickup_get_task", return_value=elsewhere), \
             mock.patch.object(clickup, "clickup_update_task") as put:
            with pytest.raises(ValueError, match="different list"):
                clickup._sync_parent("tok", "T1", _cu_task_parent("T1", None), story, ctx)
        put.assert_not_called()

    def test_allows_a_parent_id_in_the_same_list(self):
        story = _story_with("child", clickup_id="T1", parent="OUTSIDE")
        ctx = self._ctx([story])
        same = _cu_task_parent("OUTSIDE", None)
        same["list"] = {"id": "999999"}  # the project's list
        with mock.patch.object(clickup, "clickup_get_task", return_value=same), \
             mock.patch.object(clickup, "clickup_update_task") as put:
            assert clickup._sync_parent(
                "tok", "T1", _cu_task_parent("T1", None), story, ctx
            ) is True
        put.assert_called_once_with("tok", "T1", {"parent": "OUTSIDE"})

    def test_in_file_parent_costs_no_extra_fetch(self):
        stories = [
            _story_with("Export hub", clickup_id="HUB"),
            _story_with("child", clickup_id="T1", parent="Export hub"),
        ]
        ctx = self._ctx(stories, cu_by_id={"HUB": _cu_task_parent("HUB", None)})
        with mock.patch.object(clickup, "clickup_get_task") as get, \
             mock.patch.object(clickup, "clickup_update_task"):
            clickup._sync_parent("tok", "T1", _cu_task_parent("T1", None), stories[1], ctx)
        get.assert_not_called()

    def test_missing_parent_task_is_reported_not_pushed(self):
        story = _story_with("child", clickup_id="T1", parent="NOSUCHID")
        ctx = self._ctx([story])
        with mock.patch.object(clickup, "clickup_get_task",
                               side_effect=Exception("HTTP Error 404: Not Found")), \
             mock.patch.object(clickup, "clickup_update_task") as put:
            with pytest.raises(ValueError, match="could not be read"):
                clickup._sync_parent("tok", "T1", _cu_task_parent("T1", None), story, ctx)
        put.assert_not_called()

    def test_dry_run_still_checks_membership(self):
        story = _story_with("child", clickup_id="T1", parent="OUTSIDE")
        ctx = self._ctx([story])
        elsewhere = _cu_task_parent("OUTSIDE", None)
        elsewhere["list"] = {"id": "111111"}
        with mock.patch.object(clickup, "clickup_get_task", return_value=elsewhere):
            with pytest.raises(ValueError, match="different list"):
                clickup._sync_parent(
                    "tok", "T1", _cu_task_parent("T1", None), story, ctx, dry_run=True
                )


# ---------------------------------------------------------------------------
# Advisory locking
# ---------------------------------------------------------------------------
#
# The behaviours worth pinning are the ones whose failure is INVISIBLE:
#   - a session deadlocking against its own hook-held lock (looks like the lock
#     working correctly);
#   - a busy lock being treated as "nothing to do" instead of an error;
#   - a released lock deleting a hold its own session still needs.


def _lock_doc(path: Path) -> dict:
    return json.loads(path.read_text())


def _write_lock_doc(path: Path, session_id: str, age_ms: int = 0) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "session_id": session_id,
        "ts": int(time.time() * 1000) - age_ms,
        "pid": 4242,
    }))


class TestLockPaths:
    def test_project_tasks_yaml_maps_to_the_hooks_lock_file(self, tmp_path):
        """Interop is the whole point: for the file the Claude Code diamond-lock
        hook guards, this tool must land on the exact same lock path, or the two
        mechanisms each hold a file the other is writing."""
        yaml_file = tmp_path / "docs" / "project-tasks.yaml"
        yaml_file.parent.mkdir(parents=True)
        yaml_file.write_text("epics: []\n")
        assert clickup.lock_path_for_yaml(str(yaml_file)) == yaml_file.parent / ".project-tasks.lock"

    def test_other_file_names_get_their_own_lock_without_a_second_convention(self, tmp_path):
        f = tmp_path / "roadmap.yaml"
        f.write_text("epics: []\n")
        assert clickup.lock_path_for_yaml(str(f)) == tmp_path / ".roadmap.lock"

    def test_list_lock_is_machine_global_not_next_to_the_file(self, tmp_path, monkeypatch):
        """Two different YAML files aimed at one ClickUp list must contend. A
        lock beside either file would not make them."""
        monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
        p = clickup.lock_path_for_list("901419115649")
        assert p == tmp_path / "cache" / "clickup-yaml-sync" / "list-901419115649.lock"


class TestLockOwnerIdentity:
    def test_explicit_override_wins(self, monkeypatch):
        monkeypatch.setenv("CLICKUP_LOCK_OWNER", "cron-nightly")
        monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", "sess-1")
        assert clickup.lock_owner_id() == "cron-nightly"

    def test_claude_session_id_is_the_identity_under_claude_code(self, monkeypatch):
        """Verified against a live hook-written lock on 2026-08-21: the hook
        writes its payload's session_id, and CLAUDE_CODE_SESSION_ID in a Bash
        tool call holds that same UUID. (CLAUDE_SESSION_ID, without CODE, is a
        different and empty variable -- do not switch to it.)"""
        monkeypatch.delenv("CLICKUP_LOCK_OWNER", raising=False)
        monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", "1af01d76-5f32-4e59-8165-43f9252a4a5a")
        assert clickup.lock_owner_id() == "1af01d76-5f32-4e59-8165-43f9252a4a5a"

    def test_plain_shell_identity_is_unique_per_process(self, monkeypatch):
        """Deliberately NOT a bare pid or hostname: a stable-looking id could
        collide with a real session's lock, and a colliding id steals a lock
        instead of waiting for it."""
        monkeypatch.delenv("CLICKUP_LOCK_OWNER", raising=False)
        monkeypatch.delenv("CLAUDE_CODE_SESSION_ID", raising=False)
        a, b = clickup.lock_owner_id(), clickup.lock_owner_id()
        assert a != b
        assert a.startswith("shell-")


class TestLockAcquisition:
    def test_acquires_when_free_and_removes_on_release(self, tmp_path):
        path = tmp_path / ".project-tasks.lock"
        lock = clickup.AdvisoryLock(path, "me", "YAML file")
        lock.acquire(wait_seconds=0)
        assert _lock_doc(path)["session_id"] == "me"
        lock.release()
        assert not path.exists()

    def test_own_sessions_fresh_lock_is_adopted_not_waited_on(self, tmp_path):
        """The self-deadlock case, and the one that looks like success. A Claude
        session edits (the hook takes the lock), then syncs in the same session;
        the sync must join that hold, not block on it for the full TTL."""
        path = tmp_path / ".project-tasks.lock"
        _write_lock_doc(path, "sess-A", age_ms=1000)
        lock = clickup.AdvisoryLock(path, "sess-A", "YAML file")
        lock.acquire(wait_seconds=0)  # would raise LockBusy if it treated it as foreign
        assert lock.inherited is True

    def test_releasing_an_adopted_lock_hands_it_back_rather_than_deleting_it(self, tmp_path):
        """The session may still be mid-edit-burst. Deleting here would silently
        cut short a hold it is relying on, and the next hook edit would find the
        file free."""
        path = tmp_path / ".project-tasks.lock"
        _write_lock_doc(path, "sess-A", age_ms=1000)
        lock = clickup.AdvisoryLock(path, "sess-A", "YAML file")
        lock.acquire(wait_seconds=0)
        before = _lock_doc(path)["ts"]
        lock.release()
        assert path.exists(), "an adopted lock must survive release"
        assert _lock_doc(path)["session_id"] == "sess-A"
        assert _lock_doc(path)["ts"] >= before, "and be refreshed, so the sync counts as activity"

    def test_stale_lock_is_taken_over(self, tmp_path):
        path = tmp_path / ".project-tasks.lock"
        _write_lock_doc(path, "dead-session", age_ms=clickup.LOCK_TTL_MS + 5000)
        lock = clickup.AdvisoryLock(path, "me", "YAML file")
        lock.acquire(wait_seconds=0)
        assert _lock_doc(path)["session_id"] == "me"

    def test_foreign_fresh_lock_fails_loudly_and_changes_nothing(self, tmp_path):
        """Never a quiet skip. A sync that silently does nothing is the failure
        mode that hides for days."""
        path = tmp_path / ".project-tasks.lock"
        _write_lock_doc(path, "sess-OTHER", age_ms=1000)
        lock = clickup.AdvisoryLock(path, "me", "YAML file")
        with pytest.raises(clickup.LockBusy) as e:
            lock.acquire(wait_seconds=0)
        assert "sess-OTH" in str(e.value)
        assert "--lock-timeout" in str(e.value), "the message must say how to wait longer"
        assert str(path) in str(e.value), "and where the lock is, for a crashed holder"
        assert _lock_doc(path)["session_id"] == "sess-OTHER", "the holder's lock is untouched"

    def test_waits_for_a_lock_that_is_released_and_then_succeeds(self, tmp_path, monkeypatch):
        """The ordinary case Maurice described: you do not have the flag, so you
        wait, and the wait is short."""
        import threading

        path = tmp_path / ".project-tasks.lock"
        _write_lock_doc(path, "sess-OTHER", age_ms=1000)
        monkeypatch.setattr(clickup, "LOCK_POLL_SECONDS", 0.01)
        threading.Timer(0.05, path.unlink).start()  # the holder finishes
        lock = clickup.AdvisoryLock(path, "me", "YAML file")
        lock.acquire(wait_seconds=10)
        assert _lock_doc(path)["session_id"] == "me"

    def test_refresh_keeps_a_long_run_from_going_stale_under_its_own_feet(self, tmp_path):
        """A board big enough to sync for longer than the TTL would otherwise
        let another session walk in behind it mid-run."""
        path = tmp_path / ".project-tasks.lock"
        lock = clickup.AdvisoryLock(path, "me", "YAML file")
        lock.acquire(wait_seconds=0)
        _write_lock_doc(path, "me", age_ms=clickup.LOCK_TTL_MS + 1000)  # simulate a slow run
        assert clickup._lock_age_ms(_lock_doc(path)) >= clickup.LOCK_TTL_MS
        lock.refresh()
        assert clickup._lock_age_ms(_lock_doc(path)) < 1000

    def test_does_not_release_a_lock_that_was_taken_over(self, tmp_path):
        """Our TTL lapsed and someone else took it. Deleting would strip a lock
        off a run that is actively using it."""
        path = tmp_path / ".project-tasks.lock"
        lock = clickup.AdvisoryLock(path, "me", "YAML file")
        lock.acquire(wait_seconds=0)
        _write_lock_doc(path, "sess-NEW", age_ms=0)
        lock.release()
        assert _lock_doc(path)["session_id"] == "sess-NEW"

    def test_a_corrupt_lock_file_reads_as_free(self, tmp_path):
        """Same as the hook. An advisory lock that failed closed on a corrupt
        file would wedge the tool with no way out but a manual delete."""
        path = tmp_path / ".project-tasks.lock"
        path.write_text("{not json")
        clickup.AdvisoryLock(path, "me", "YAML file").acquire(wait_seconds=0)
        assert _lock_doc(path)["session_id"] == "me"

    def test_lock_write_is_atomic_so_a_reader_never_sees_a_half_file(self, tmp_path):
        path = tmp_path / ".project-tasks.lock"
        clickup._write_lock(path, "me")
        assert not list(tmp_path.glob("*.tmp*")), "no temp file left behind"
        assert _lock_doc(path)["session_id"] == "me"


class TestSyncLockComposite:
    def test_holds_both_file_and_list_locks_and_releases_both(self, tmp_path, monkeypatch):
        monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
        yaml_file = tmp_path / "project-tasks.yaml"
        yaml_file.write_text("epics: []\n")
        file_lock = tmp_path / ".project-tasks.lock"
        list_lock = clickup.lock_path_for_list("L1")
        with clickup.SyncLock(str(yaml_file), "L1", wait_seconds=0, owner="me"):
            assert _lock_doc(file_lock)["session_id"] == "me"
            assert _lock_doc(list_lock)["session_id"] == "me"
        assert not file_lock.exists()
        assert not list_lock.exists()

    def test_a_busy_list_lock_releases_the_file_lock_it_already_took(self, tmp_path, monkeypatch):
        """Otherwise a failed acquire leaves the file locked by a process that
        is no longer running -- for a full TTL, for nothing."""
        monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
        yaml_file = tmp_path / "project-tasks.yaml"
        yaml_file.write_text("epics: []\n")
        _write_lock_doc(clickup.lock_path_for_list("L1"), "sess-OTHER", age_ms=1000)
        with pytest.raises(clickup.LockBusy):
            with clickup.SyncLock(str(yaml_file), "L1", wait_seconds=0, owner="me"):
                pass  # pragma: no cover
        assert not (tmp_path / ".project-tasks.lock").exists()

    def test_no_list_id_means_only_the_file_lock(self, tmp_path):
        yaml_file = tmp_path / "project-tasks.yaml"
        yaml_file.write_text("epics: []\n")
        lock = clickup.SyncLock(str(yaml_file), None, wait_seconds=0, owner="me")
        assert len(lock.locks) == 1

    def test_locks_release_even_when_the_body_raises(self, tmp_path):
        yaml_file = tmp_path / "project-tasks.yaml"
        yaml_file.write_text("epics: []\n")
        with pytest.raises(RuntimeError):
            with clickup.SyncLock(str(yaml_file), None, wait_seconds=0, owner="me"):
                raise RuntimeError("sync blew up")
        assert not (tmp_path / ".project-tasks.lock").exists()


class TestWithLockCommand:
    def test_child_inherits_the_owner_id_so_a_nested_sync_does_not_deadlock(self, monkeypatch):
        """Without this a plain shell deadlocks against itself: each process
        mints a unique owner, so `with-lock ... -- clickup.py sync` would block
        on a lock its own parent holds."""
        monkeypatch.setenv("CLICKUP_LOCK_OWNER", "owner-X")
        seen = {}

        def _run(argv, env=None):
            seen.update(env or {})
            return mock.Mock(returncode=0)

        with mock.patch.object(clickup.subprocess, "run", side_effect=_run):
            assert clickup.cmd_with_lock(["true"]) == 0
        assert seen["CLICKUP_LOCK_OWNER"] == "owner-X"

    def test_returns_the_childs_exit_code(self):
        assert clickup.cmd_with_lock(["sh", "-c", "exit 7"]) == 7

    def test_missing_command_is_reported_not_swallowed(self):
        assert clickup.cmd_with_lock(["definitely-not-a-real-command-xyz"]) == 127


class TestPeekListId:
    def test_reads_the_list_id_without_a_full_load(self, tmp_path):
        f = tmp_path / "project-tasks.yaml"
        f.write_text("project:\n  clickup_list_id: 901419115649\nepics: []\n")
        assert clickup._peek_list_id(str(f)) == "901419115649"

    def test_unreadable_file_is_not_the_lockers_error_to_raise(self, tmp_path):
        f = tmp_path / "project-tasks.yaml"
        f.write_text("{{{ not yaml")
        assert clickup._peek_list_id(str(f)) is None


class TestAtomicYamlSave:
    def test_save_leaves_no_temp_file_behind(self, tmp_path):
        f = tmp_path / "project-tasks.yaml"
        data = {"project": {"name": "p"}, "epics": []}
        clickup.save_yaml(data, str(f))
        assert yaml.safe_load(f.read_text())["epics"] == []
        assert not list(tmp_path.glob("*.tmp*"))


# ---------------------------------------------------------------------------
# Milestone-date lint
# ---------------------------------------------------------------------------
#
# The lint reports a guideline ClickUp does not enforce. The behaviours worth
# pinning are the three Maurice stated as requirements -- flag never modify,
# exceptions must be possible and self-documenting, missing data stays silent --
# plus the resolution rules, because a tag that silently resolves to nothing is
# the failure that makes the whole check look clean when it is not running.


def _lint_data(*epics: dict) -> dict:
    return {"project": {"name": "p"}, "status_map": {}, "epics": list(epics)}


def _gate(name: str, tag: str, due: str | None = None, **extra) -> dict:
    return _story_with(name, milestone=True, tags=[tag], due_date=due, **extra)


def _codes(result: dict) -> list[str]:
    return sorted(f["code"] for f in result["findings"])


class TestMilestoneLintResolution:
    def test_reads_both_free_form_tags_and_the_milestone_label_enum(self):
        refs = clickup._milestone_refs({"tags": ["m2-system", "not-a-milestone"],
                                        "milestone_label": "M1"})
        assert sorted((n, s) for n, s, _ in refs) == [(1, None), (2, "system")]

    def test_ignores_tags_that_only_look_like_milestones(self):
        assert clickup._milestone_refs({"tags": ["mx", "milestone", "s1", "m"]}) == []

    def test_a_tag_pointing_at_no_gate_is_reported(self):
        """The most valuable finding: a typo'd tag otherwise filters to nothing
        and the plan looks clean because nothing is being checked."""
        data = _lint_data(_epic_with("Delivery", [
            _story_with("orphan", tags=["m7-nope"], due_date="2026-01-01"),
        ]))
        assert clickup.LINT_TAG_UNRESOLVED in _codes(clickup.lint_milestone_dates(data))

    def test_two_gates_sharing_a_number_are_reported_as_ambiguous(self):
        data = _lint_data(_epic_with("Milestones", [
            _gate("M1 a", "m1-alpha", "2026-09-01"),
            _gate("M1 b", "m1-beta", "2026-09-02"),
        ]))
        assert _codes(clickup.lint_milestone_dates(data)) == [
            clickup.LINT_AMBIGUOUS, clickup.LINT_AMBIGUOUS,
        ]

    def test_a_card_under_an_ambiguous_number_is_not_date_checked(self):
        """It cannot be resolved to one gate, so a date verdict would be a
        coin flip presented as a finding."""
        data = _lint_data(
            _epic_with("Milestones", [
                _gate("M1 a", "m1-alpha", "2026-09-01"),
                _gate("M1 b", "m1-beta", "2026-12-01"),
            ]),
            _epic_with("Delivery", [
                _story_with("late", tags=["m1-alpha"], due_date="2026-11-01"),
            ]),
        )
        assert clickup.LINT_DATE_AFTER_GATE not in _codes(clickup.lint_milestone_dates(data))

    def test_same_number_different_slug_is_flagged_but_still_checked(self):
        """A typo'd slug must not silently disable the date check -- that would
        turn a typo into an invisible gap."""
        data = _lint_data(
            _epic_with("Milestones", [_gate("M1", "m1-infrastructure", "2026-09-15")]),
            _epic_with("Delivery", [
                _story_with("typo", tags=["m1-infrastrucutre"], due_date="2026-10-01"),
            ]),
        )
        assert _codes(clickup.lint_milestone_dates(data)) == [
            clickup.LINT_DATE_AFTER_GATE, clickup.LINT_SLUG_MISMATCH,
        ]

    def test_a_milestone_with_no_tag_cannot_be_referenced_and_is_reported(self):
        data = _lint_data(_epic_with("Milestones", [
            _story_with("nameless gate", milestone=True, due_date="2026-09-01"),
        ]))
        assert clickup.LINT_TAG_UNRESOLVED in _codes(clickup.lint_milestone_dates(data))

    def test_epics_are_never_used_to_infer_a_milestone(self):
        """A diamond is deliberately not filed under a work epic, so sharing an
        epic must imply nothing."""
        data = _lint_data(_epic_with("Delivery", [
            _gate("M1", "m1-infra", "2026-09-01"),
            _story_with("untagged sibling", due_date="2026-12-01"),
        ]))
        assert clickup.lint_milestone_dates(data)["findings"] == []


class TestMilestoneLintDates:
    def test_due_after_the_gate_is_a_contradiction(self):
        data = _lint_data(
            _epic_with("Milestones", [_gate("M1", "m1-infra", "2026-09-15")]),
            _epic_with("Delivery", [_story_with("late", tags=["m1-infra"], due_date="2026-10-01")]),
        )
        findings = clickup.lint_milestone_dates(data)["findings"]
        assert [f["code"] for f in findings] == [clickup.LINT_DATE_AFTER_GATE]
        assert findings[0]["severity"] == "error"

    def test_due_on_the_gate_date_is_fine(self):
        """On or before. A card finishing on the gate day is coherent."""
        data = _lint_data(
            _epic_with("Milestones", [_gate("M1", "m1-infra", "2026-09-15")]),
            _epic_with("Delivery", [_story_with("ok", tags=["m1-infra"], due_date="2026-09-15")]),
        )
        assert clickup.lint_milestone_dates(data)["findings"] == []

    def test_a_card_with_no_due_date_is_silent(self):
        """Today most cards have no date at all. Noise here would bury the real
        findings on every board in the account."""
        data = _lint_data(
            _epic_with("Milestones", [_gate("M1", "m1-infra", "2026-09-15")]),
            _epic_with("Delivery", [_story_with("undated", tags=["m1-infra"])]),
        )
        assert clickup.lint_milestone_dates(data)["findings"] == []

    def test_an_undated_gate_is_a_note_once_not_a_violation_per_card(self):
        data = _lint_data(
            _epic_with("Milestones", [_gate("M1", "m1-infra")]),
            _epic_with("Delivery", [
                _story_with("a", tags=["m1-infra"], due_date="2026-10-01"),
                _story_with("b", tags=["m1-infra"], due_date="2026-11-01"),
            ]),
        )
        findings = clickup.lint_milestone_dates(data)["findings"]
        assert [f["code"] for f in findings] == [clickup.LINT_GATE_UNDATED]
        assert findings[0]["severity"] == "info"

    def test_a_gate_is_not_checked_against_itself(self):
        data = _lint_data(_epic_with("Milestones", [_gate("M1", "m1-infra", "2026-09-15")]))
        assert clickup.lint_milestone_dates(data)["findings"] == []


class TestMilestoneLintExceptions:
    def _late(self, **story_extra) -> dict:
        return _lint_data(
            _epic_with("Milestones", [_gate("M1", "m1-infra", "2026-09-15")]),
            _epic_with("Delivery", [
                _story_with("late", tags=["m1-infra"], due_date="2026-10-01", **story_extra),
            ]),
        )

    def test_a_written_reason_accepts_the_finding(self):
        result = clickup.lint_milestone_dates(self._late(
            lint_exceptions={"milestone-date": "Client moved the window, agreed 2026-08-20"},
        ))
        assert result["findings"] == []
        assert len(result["accepted"]) == 1
        assert "Client moved the window" in result["accepted"][0]["accepted_because"]

    def test_a_bare_true_is_not_an_exception(self):
        """A suppression flag with no rationale tells the next reader nothing,
        and accepting one would let the self-documenting rule rot away a card at
        a time."""
        result = clickup.lint_milestone_dates(self._late(lint_exceptions={"milestone-date": True}))
        assert _codes(result) == [clickup.LINT_DATE_AFTER_GATE]

    def test_an_empty_reason_is_not_an_exception(self):
        result = clickup.lint_milestone_dates(self._late(lint_exceptions={"milestone-date": "  "}))
        assert _codes(result) == [clickup.LINT_DATE_AFTER_GATE]

    def test_an_exception_for_a_different_code_does_not_suppress_this_one(self):
        result = clickup.lint_milestone_dates(self._late(
            lint_exceptions={"milestone-tag-unresolved": "different finding entirely"},
        ))
        assert _codes(result) == [clickup.LINT_DATE_AFTER_GATE]

    def test_accepted_findings_are_counted_not_discarded(self):
        """Suppressed is not the same as gone -- the report still says how many."""
        result = clickup.lint_milestone_dates(self._late(
            lint_exceptions={"milestone-date": "known and agreed"},
        ))
        assert len(result["accepted"]) == 1


class TestMilestoneLintIsAdvisory:
    def test_the_lint_never_modifies_the_data_it_reads(self):
        """Flag, never modify. A date is a human's decision."""
        data = _lint_data(
            _epic_with("Milestones", [_gate("M1", "m1-infra", "2026-09-15")]),
            _epic_with("Delivery", [_story_with("late", tags=["m1-infra"], due_date="2026-10-01")]),
        )
        before = copy.deepcopy(data)
        clickup.lint_milestone_dates(data)
        assert data == before

    def test_cmd_lint_exits_zero_even_with_findings(self, capsys):
        """A lint that breaks a build over a guideline gets routed around, and a
        routed-around lint is worse than none."""
        data = _lint_data(
            _epic_with("Milestones", [_gate("M1", "m1-infra", "2026-09-15")]),
            _epic_with("Delivery", [_story_with("late", tags=["m1-infra"], due_date="2026-10-01")]),
        )
        assert clickup.cmd_lint(data) == 0
        assert "CONTRADICTION" in capsys.readouterr().out

    def test_strict_is_the_opt_in_for_a_non_zero_exit(self):
        data = _lint_data(
            _epic_with("Milestones", [_gate("M1", "m1-infra", "2026-09-15")]),
            _epic_with("Delivery", [_story_with("late", tags=["m1-infra"], due_date="2026-10-01")]),
        )
        assert clickup.cmd_lint(data, strict=True) == 1

    def test_strict_still_exits_zero_when_clean(self):
        assert clickup.cmd_lint(_lint_data(), strict=True) == 0

    def test_a_clean_board_prints_nothing_on_the_advisory_tail(self, capsys):
        clickup.print_lint_report(clickup.lint_milestone_dates(_lint_data()))
        assert capsys.readouterr().out == ""

    def test_an_empty_project_does_not_crash(self):
        assert clickup.lint_milestone_dates({})["findings"] == []


# ---------------------------------------------------------------------------
# milestone_label accepts a slug (M<n>-<slug>), not just the old M0-M3 enum
# ---------------------------------------------------------------------------
#
# The field exists to spare people hand-writing the tag. While it emitted a bare
# `m1` -- a handle that tells a reader nothing -- every project bypassed it and
# hand-rolled its own convention, and hand-rolled conventions diverge. Widening
# it makes the agreed shape the default instead of a habit each project has to
# remember.


class TestMilestoneLabelSlug:
    def test_a_slugged_label_pushes_as_a_lowercased_slug_tag(self):
        story = _story_with("card", milestone_label="M1-infrastructure")
        epic = _epic_with("Delivery", [story])
        assert "m1-infrastructure" in clickup._story_desired_tags(story, epic)

    def test_the_bare_form_is_unchanged(self):
        """The Michael Moe board uses the bare form on live cards today and must
        not be disturbed."""
        story = _story_with("card", milestone_label="M1")
        epic = _epic_with("Delivery", [story])
        assert "m1" in clickup._story_desired_tags(story, epic)

    def test_a_slugged_label_is_in_the_managed_tag_universe(self):
        """Otherwise reassigning a card's milestone would leave the old tag
        behind on the ClickUp task forever."""
        data = _lint_data(_epic_with("Delivery", [
            _story_with("card", milestone_label="M2-system"),
        ]))
        assert "m2-system" in clickup._collect_managed_tag_universe(data)

    def test_the_bare_slugs_stay_permanently_managed(self):
        """Backward compatibility: removing `milestone_label: M1` must still
        strip the `m1` tag even with no base snapshot to diff against."""
        universe = clickup._collect_managed_tag_universe(_lint_data())
        assert {"m0", "m1", "m2", "m3"} <= universe

    def test_the_number_is_unbounded(self):
        """The old enum capped a project at four milestones for no reason a
        five-milestone SOW would accept."""
        story = _story_with("card", milestone_label="M7-handover")
        epic = _epic_with("Delivery", [story])
        assert "m7-handover" in clickup._story_desired_tags(story, epic)

    def test_push_and_the_lint_share_one_definition_of_a_milestone_slug(self):
        """Two regexes for one convention would drift, and the drift would show
        up as a lint that quietly stops resolving what push emits."""
        story = {"milestone_label": "M1-infrastructure"}
        epic = _epic_with("Delivery", [story])
        emitted = [t for t in clickup._story_desired_tags(story, epic) if t.startswith("m1")]
        assert clickup._milestone_refs(story) == [(1, "infrastructure", "m1-infrastructure")]
        assert emitted == ["m1-infrastructure"]

    def test_a_slugged_label_resolves_to_its_gate_in_the_lint(self):
        """Confirmed by running it, not inferred: the widened field needs no
        further lint work."""
        data = _lint_data(
            _epic_with("Milestones", [
                _story_with("M1 gate", milestone=True, milestone_label="M1-infrastructure",
                            due_date="2026-09-15"),
            ]),
            _epic_with("Delivery", [
                _story_with("late", milestone_label="M1-infrastructure", due_date="2026-10-01"),
            ]),
        )
        assert _codes(clickup.lint_milestone_dates(data)) == [clickup.LINT_DATE_AFTER_GATE]

    def test_a_label_and_an_equivalent_tag_are_one_reference_not_two(self):
        refs = clickup._milestone_refs({
            "milestone_label": "M1-infrastructure",
            "tags": ["m1-infrastructure"],
        })
        assert refs == [(1, "infrastructure", "m1-infrastructure")]

    def test_a_malformed_label_is_flagged_by_the_lint(self):
        """It would push a tag nothing can resolve, so the card looks tagged and
        is silently unchecked -- worth catching at the source."""
        data = _lint_data(_epic_with("Delivery", [
            _story_with("card", milestone_label="Milestone 1"),
        ]))
        assert clickup.LINT_LABEL_MALFORMED in _codes(clickup.lint_milestone_dates(data))

    def test_a_well_formed_label_is_not_flagged(self):
        data = _lint_data(_epic_with("Delivery", [
            _story_with("a", milestone_label="M1"),
            _story_with("b", milestone_label="M12-late-stage-handover"),
        ]))
        codes = _codes(clickup.lint_milestone_dates(data))
        assert clickup.LINT_LABEL_MALFORMED not in codes

    def test_the_schema_pattern_matches_what_the_tool_accepts(self):
        """A schema that documents a different rule from the code is worse than
        no schema -- it is believed."""
        import re as _re
        schema = yaml.safe_load((Path(clickup.__file__).parent / "schema.yaml").read_text())
        node = schema["properties"]["epics"]["items"]["properties"]["stories"]["items"]
        pattern = node["properties"]["milestone_label"]["pattern"]
        for good in ("M1", "M0", "M1-infrastructure", "M12-late-stage-handover"):
            assert _re.match(pattern, good), good
            assert clickup.MILESTONE_TAG_RE.match(good), good
        for bad in ("Milestone 1", "M", "m1-", "M1 infrastructure", "M1_infra"):
            assert not _re.match(pattern, bad), bad
            assert not clickup.MILESTONE_TAG_RE.match(bad), bad


# ---------------------------------------------------------------------------
# YAML-only story fields (`notes:`)
# ---------------------------------------------------------------------------
#
# These pass against the code as it already stands -- unknown story keys survive
# every path today, because push and pull both work from explicit field lists
# and mutate story dicts in place rather than rebuilding them.
#
# That is precisely why the tests exist. The behaviour is emergent, and the day
# someone rebuilds a story dict or adds a field to comparable_local, this
# material disappears with no error and no diff entry. It is the material people
# put here BECAUSE it was too valuable to delete, so the failure would be silent
# data loss of the worst kind. These tests turn an accident into a contract.


def _noted_story(**extra) -> dict:
    return _story_with(
        "Card",
        clickup_id="T1",
        description="short and readable",
        notes="Eric said on the kick-off call that the threshold is split "
              "routine vs adversarial; SOW clause 4b.",
        **extra,
    )


def _noted_cu_task() -> dict:
    return {
        "id": "T1", "name": "Card", "status": {"status": "backlog"},
        "description": "short and readable", "tags": [], "priority": None,
        "due_date": None, "start_date": None, "custom_fields": [],
        "assignees": [], "date_updated": "1", "url": "u",
    }


class TestYamlOnlyStoryFields:
    def test_notes_is_declared_yaml_only(self):
        assert "notes" in clickup.YAML_ONLY_STORY_FIELDS

    def test_pull_does_not_delete_notes(self):
        """The headline failure: write notes, run pull, notes vanish because the
        remote has no counterpart."""
        story = _noted_story()
        clickup._apply_clickup_to_yaml(story, _noted_cu_task(), {})
        assert story["notes"].startswith("Eric said")

    def test_notes_never_enters_the_comparison(self):
        """It has no remote counterpart, so it can never conflict. If it reached
        compare_task it would report a permanent phantom difference."""
        assert "notes" not in clickup.comparable_local(_noted_story(), {})

    def test_notes_never_enters_the_base_snapshot(self):
        data = _lint_data(_epic_with("E", [_noted_story()]))
        assert "notes" not in clickup.build_base_from_yaml(data, {})["T1"]

    def test_notes_is_not_appended_to_the_pushed_description(self):
        """The whole point is that a reader of the ClickUp card never sees it."""
        assert "Eric said" not in clickup.description_with_meta(_noted_story())

    def test_notes_does_not_become_a_tag(self):
        story = _noted_story()
        epic = _epic_with("E", [story])
        assert not any("eric" in t.lower() for t in clickup._story_desired_tags(story, epic))

    def test_notes_is_not_in_the_create_or_update_api_body(self):
        """The last line of defence: whatever else happens, it must not be sent."""
        story = _noted_story()
        body = clickup.build_task_body(story, _epic_with("E", [story]), {}, {})
        assert "Eric said" not in json.dumps(body)
        assert "notes" not in body

    def test_notes_survives_a_full_save_and_reload(self, tmp_path):
        f = tmp_path / "project-tasks.yaml"
        data = _lint_data(_epic_with("E", [_noted_story()]))
        clickup.save_yaml(data, str(f))
        reloaded = clickup.load_yaml(str(f))
        assert reloaded["epics"][0]["stories"][0]["notes"].startswith("Eric said")

    def test_a_story_imported_from_clickup_has_no_notes_rather_than_a_fake_one(self):
        """A card that arrives from ClickUp has no provenance to record. An empty
        string would look authored; absent is honest."""
        story = clickup._clickup_task_to_yaml_story(_noted_cu_task(), {})
        assert "notes" not in story

    def test_merge_resolution_does_not_disturb_notes(self):
        """cmd_merge writes resolved values field by field into the existing
        story dict -- pinned so a future rewrite cannot start replacing it."""
        story = _noted_story()
        with mock.patch.object(clickup, "clickup_update_task"):
            clickup._apply_merged_value(story, _noted_cu_task(), "name", "New name", {}, "tok")
        assert story["name"] == "New name"
        assert story["notes"].startswith("Eric said")

    def test_pull_writes_nothing_into_notes(self):
        """Decision: notes is authored, not derived. A pull that appended to it
        would make it untrustworthy -- you could no longer tell what a human
        meant from what a sync deposited."""
        story = _noted_story()
        before = story["notes"]
        clickup._apply_clickup_to_yaml(story, _noted_cu_task(), {})
        clickup._sync_metadata(story, _noted_cu_task())
        assert story["notes"] == before


# ---------------------------------------------------------------------------
# API error detail, and edge-failure visibility
# ---------------------------------------------------------------------------
#
# The failure this prevents: a board syncs its tasks fine, every declared
# dependency silently fails to be created, and the run reports success. On a
# 13-card board that is 13 warnings scrolling past a summary that says nothing
# is wrong -- and the resulting board is missing structure the YAML declares.


@pytest.fixture(autouse=True)
def _reset_edge_failures():
    clickup._EDGE_FAILURES.clear()
    clickup._EDGE_HINT_SHOWN = False
    yield
    clickup._EDGE_FAILURES.clear()
    clickup._EDGE_HINT_SHOWN = False


class TestClickUpAPIError:
    def test_the_error_message_carries_the_api_reason_not_just_the_status(self):
        """urllib's HTTPError stringifies to 'HTTP Error 403: Forbidden' and the
        body is already consumed by the time a caller sees it -- so every
        `except Exception as e: log(...{e})` printed the status and threw away
        the only part that says why."""
        e = clickup.ClickUpAPIError(
            403, '{"err":"Dependencies are not enabled","ECODE":"OAUTH_027"}',
            "POST", "https://api.clickup.com/api/v2/task/T1/dependency",
        )
        assert "Dependencies are not enabled" in str(e)
        assert "OAUTH_027" in str(e)
        assert e.ecode == "OAUTH_027"
        assert e.status == 403

    def test_a_non_json_body_still_produces_a_usable_message(self):
        e = clickup.ClickUpAPIError(502, "<html>Bad Gateway</html>", "POST", "u")
        assert "502" in str(e)
        assert "Bad Gateway" in str(e)

    def test_an_empty_body_does_not_produce_a_bare_colon(self):
        assert "(no body)" in str(clickup.ClickUpAPIError(500, "", "POST", "u"))


class TestEdgeFailureVisibility:
    def _story(self) -> dict:
        return _story_with("Gate work", clickup_id="T1", depends_on=["T2"])

    def _cu(self) -> dict:
        return {"id": "T1", "name": "Gate work", "dependencies": [], "linked_tasks": []}

    def test_a_failed_dependency_is_recorded_for_the_end_of_run_summary(self):
        err = clickup.ClickUpAPIError(403, '{"err":"no","ECODE":"X"}', "POST", "u")
        with mock.patch.object(clickup, "clickup_add_dependency", side_effect=err):
            clickup._sync_dependencies("tok", "T1", self._cu(), self._story())
        assert len(clickup._EDGE_FAILURES) == 1
        assert "T2" in clickup._EDGE_FAILURES[0]

    def test_a_failed_edge_does_not_abort_the_rest_of_the_sync(self):
        """Tasks still sync. The point is that the failure is reported, not that
        it becomes fatal."""
        err = clickup.ClickUpAPIError(403, "{}", "POST", "u")
        with mock.patch.object(clickup, "clickup_add_dependency", side_effect=err):
            clickup._sync_dependencies("tok", "T1", self._cu(), self._story())  # no raise

    def test_the_checklist_is_shown_once_per_run_not_once_per_edge(self):
        """Thirteen cards failing the same way should not print the same four
        bullet points thirteen times."""
        err = clickup.ClickUpAPIError(403, "{}", "POST", "u")
        story = _story_with("s", clickup_id="T1", depends_on=["T2", "T3", "T4"])
        with mock.patch.object(clickup, "clickup_add_dependency", side_effect=err):
            clickup._sync_dependencies("tok", "T1", self._cu(), story)
        assert len(clickup._EDGE_FAILURES) == 3
        assert clickup._EDGE_HINT_SHOWN is True

    def test_a_failed_relation_is_recorded_too(self):
        err = clickup.ClickUpAPIError(403, "{}", "POST", "u")
        story = _story_with("s", clickup_id="T1", related=["T2"])
        with mock.patch.object(clickup, "clickup_add_link", side_effect=err):
            clickup._sync_relations("tok", "T1", self._cu(), story)
        assert len(clickup._EDGE_FAILURES) == 1

    def test_the_summary_names_every_failed_edge(self, capsys):
        clickup._EDGE_FAILURES.extend(["add dependency: 'a' -> T2: boom",
                                       "add dependency: 'b' -> T3: boom"])
        clickup.report_edge_failures()
        out = capsys.readouterr().out
        assert "EDGE OPERATIONS FAILED: 2" in out
        assert "T2" in out and "T3" in out
        assert "missing structure the YAML declares" in out

    def test_the_summary_is_silent_when_nothing_failed(self, capsys):
        clickup.report_edge_failures()
        assert capsys.readouterr().out == ""

    def test_the_checklist_does_not_claim_to_diagnose(self):
        """These are plausible causes on this account, not a reading of the
        error. Presenting a guess as a diagnosis sends people down the wrong
        path with confidence."""
        assert "checklist, not a diagnosis" in clickup.EDGE_FAILURE_HINT
        assert "Dependencies ClickApp" in clickup.EDGE_FAILURE_HINT
        assert "INSUFFICIENT_ACCESS" in clickup.EDGE_FAILURE_HINT


# ---------------------------------------------------------------------------
# depends_on / related may reference stories by NAME
# ---------------------------------------------------------------------------
#
# An id does not exist until the run that creates it. The reconcile pass runs
# AFTER creates, so a NAME reference links two brand-new tasks on the FIRST sync
# where an id reference could not — which is the common case for any new board,
# and previously forced a second pass. `parent` already worked this way; this
# extends the same machinery to the edge fields.


def _edge_ctx(*stories: dict) -> dict:
    return clickup._build_parent_context(
        {"project": {"clickup_list_id": "L"}, "epics": [_epic_with("E", list(stories))]}
    )


class TestEdgeRefsByName:
    def test_a_story_name_resolves_to_its_clickup_id(self):
        ctx = _edge_ctx(_story_with("Export hub", clickup_id="HUB"),
                        _story_with("Downstream", clickup_id="T1"))
        assert clickup._resolve_dependency_ids(["Export hub"], "T1", ctx)[0] == ["HUB"]

    def test_a_raw_clickup_id_still_works(self):
        """Targets outside this YAML have no name to reference."""
        ctx = _edge_ctx(_story_with("Downstream", clickup_id="T1"))
        assert clickup._resolve_dependency_ids(["EXTERNAL9"], "T1", ctx)[0] == ["EXTERNAL9"]

    def test_matching_is_case_insensitive_and_trimmed(self):
        ctx = _edge_ctx(_story_with("Export hub", clickup_id="HUB"),
                        _story_with("D", clickup_id="T1"))
        assert clickup._resolve_dependency_ids(["  export HUB "], "T1", ctx)[0] == ["HUB"]

    def test_an_id_reference_wins_over_a_name(self):
        """Same precedence as `parent`: explicit id first."""
        ctx = _edge_ctx(_story_with("HUB", clickup_id="OTHER"),
                        _story_with("x", clickup_id="HUB"))
        assert clickup._resolve_dependency_ids(["HUB"], "T1", ctx)[0] == ["HUB"]

    def test_a_duplicate_name_is_refused_not_guessed(self):
        ctx = _edge_ctx(_story_with("Twin", clickup_id="A"),
                        _story_with("Twin", clickup_id="B"))
        with pytest.raises(clickup.EdgeRefUnresolved, match="ambiguous"):
            clickup._resolve_dependency_ids(["Twin"], "T1", ctx)

    def test_an_unknown_name_is_refused_rather_than_pushed_as_an_id(self):
        """Whitespace means it was meant as a name. Pushing it to ClickUp as an
        id gets an opaque 400 back instead of a message naming the typo."""
        ctx = _edge_ctx(_story_with("Export hub", clickup_id="HUB"))
        with pytest.raises(clickup.EdgeRefUnresolved, match="no story named that"):
            clickup._resolve_dependency_ids(["No Such Story"], "T1", ctx)

    def test_the_self_check_runs_after_resolution(self):
        """Checking the raw string would let a story depend on itself by name."""
        ctx = _edge_ctx(_story_with("Downstream", clickup_id="T1"))
        desired, skipped, _ = clickup._resolve_dependency_ids(["Downstream"], "T1", ctx)
        assert desired == []
        assert skipped == ["Downstream"]

    def test_no_context_preserves_the_old_id_only_behaviour(self):
        assert clickup._resolve_dependency_ids(["HUB", "X"], "T1")[0] == ["HUB", "X"]

    def test_related_uses_the_same_resolution(self):
        ctx = _edge_ctx(_story_with("Export hub", clickup_id="HUB"),
                        _story_with("D", clickup_id="T1"))
        assert clickup._resolve_dependency_ids(["Export hub"], "T1", ctx, "related")[0] == ["HUB"]

    def test_the_error_names_the_field_it_came_from(self):
        ctx = _edge_ctx(_story_with("x", clickup_id="T1"))
        with pytest.raises(clickup.EdgeRefUnresolved, match="related:"):
            clickup._resolve_dependency_ids(["No Such Story"], "T1", ctx, "related")


class TestEdgeRefsPendingCreate:
    def _ctx_and_story(self):
        pending = _story_with("Brand new")            # no clickup_id yet
        story = _story_with("D", clickup_id="T1", depends_on=["Brand new"])
        return _edge_ctx(pending, story), story

    def test_a_name_pending_create_is_separated_from_a_real_failure(self):
        ctx, _ = self._ctx_and_story()
        desired, skipped, pending = clickup._resolve_dependency_ids(["Brand new"], "T1", ctx)
        assert (desired, skipped, pending) == ([], [], ["Brand new"])

    def test_dry_run_reports_it_as_resolving_later_rather_than_erroring(self):
        """The expected state of a fresh board: nothing created, so nothing has
        an id. Erroring here would make --dry-run useless on a new project,
        which is exactly the case the feature is for."""
        _, story = self._ctx_and_story()
        clickup._handle_pending_edge_refs("depends_on", ["Brand new"], story, dry_run=True)

    def test_a_real_run_refuses_rather_than_applying_a_partial_edge_set(self):
        """The reconcile pass runs after creates, so this should be impossible.
        A half-applied dependency graph is worse than none — it looks complete."""
        _, story = self._ctx_and_story()
        with pytest.raises(clickup.EdgeRefUnresolved, match="partial edge set"):
            clickup._handle_pending_edge_refs("depends_on", ["Brand new"], story, dry_run=False)


class TestEdgeRefsInAuxiliaryIndexes:
    def test_the_peer_relation_index_resolves_names(self):
        """It compares ids. A name on one side would silently stop matching, and
        the peer-union semantics would quietly revert to this-side-authoritative
        — bringing back the link-oscillation footgun it exists to prevent."""
        data = {"project": {"clickup_list_id": "L"}, "epics": [_epic_with("E", [
            _story_with("Export hub", clickup_id="HUB"),
            _story_with("Downstream", clickup_id="T1", related=["Export hub"]),
        ])]}
        ctx = clickup._build_parent_context(data)
        assert clickup._build_declared_relations(data, ctx) == {"T1": {"HUB"}}

    def test_the_both_edges_warning_matches_a_name_against_an_id(self):
        story = _story_with("D", clickup_id="T1",
                            depends_on=["Export hub"], related=["HUB"])
        ctx = _edge_ctx(_story_with("Export hub", clickup_id="HUB"), story)
        assert clickup._declared_edge_ids(story, "depends_on", ctx) == {"HUB"}
        assert clickup._declared_edge_ids(story, "related", ctx) == {"HUB"}

    def test_an_unresolvable_ref_does_not_abort_an_auxiliary_index(self):
        """These indexes are advisory. The authoritative complaint belongs to
        the sync functions, which raise — reporting the same typo twice, or
        letting index-building kill a run, would both be worse."""
        story = _story_with("D", clickup_id="T1", related=["No Such Story"])
        ctx = _edge_ctx(story)
        assert clickup._declared_edge_ids(story, "related", ctx) == set()
