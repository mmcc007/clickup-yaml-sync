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
            {"Magnit Monitoring System": [
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
            {"Magnit Monitoring System": [
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
            {"Magnit Monitoring System": [
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
            {"Magnit Monitoring System": [
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
            {"Magnit Monitoring System": [
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
            {"Magnit Monitoring System": [
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
        data = _data_with({"Magnit Monitoring System": [
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
        data = _data_with({"Magnit Monitoring System": [
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
        data = _data_with({"Magnit Monitoring System": [
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
