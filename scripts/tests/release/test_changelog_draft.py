"""changelog-draft.sh against a scratch repository with a bare origin."""

import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest

from scripts.tests.release import ROOT

SCRIPT = ROOT / "scripts" / "changelog-draft.sh"


class ChangelogDraft(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix=f"cld{os.getpid()}-")
        self.addCleanup(tmp.cleanup)
        root = pathlib.Path(tmp.name)
        origin, self.work = root / "origin.git", root / "work"
        env = {**os.environ, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}
        self.env = env

        def git(*args: str):
            subprocess.run(["git", "-C", str(self.work), *args], env=env, check=True, capture_output=True)

        subprocess.run(["git", "init", "-q", "--bare", "-b", "main", str(origin)], env=env, check=True)
        subprocess.run(["git", "init", "-q", "-b", "main", str(self.work)], env=env, check=True)
        git("config", "user.name", "Release test")
        git("config", "user.email", "release-test@example.invalid")
        git("remote", "add", "origin", str(origin))
        for subject, tag in (
            ("chore: first release", "v0.1.0"),
            ("feat: before the candidate", "v0.2.0-rc.1"),
            ("fix: after the candidate", None),
        ):
            git("commit", "-q", "--allow-empty", "-m", subject)
            if tag:
                git("tag", tag)
        git("push", "-q", "origin", "main", "--tags")
        (self.work / "scripts").mkdir()
        shutil.copy(SCRIPT, self.work / "scripts" / SCRIPT.name)

    def test_starts_at_the_last_final_tag_not_a_release_candidate(self):
        result = subprocess.run(
            ["bash", str(self.work / "scripts" / SCRIPT.name)],
            env=self.env, capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            ["## Unreleased (since v0.1.0)", "", "- fix: after the candidate", "- feat: before the candidate"],
        )


if __name__ == "__main__":
    unittest.main()
