#!/usr/bin/env python3
"""Offline end-to-end exact-merge size policy using real synthetic Meson libraries."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

root=Path(sys.argv[1]).resolve()
orchestrator=root/"scripts/ci/run-size-comparison.sh"
if sys.platform != "linux":
    print("test-size-comparison: SKIP requires Linux ELF and GNU size tooling")
    raise SystemExit(77)

fixture_env=os.environ.copy()
for key in tuple(fixture_env):
    if key.startswith("GIT_"):
        fixture_env.pop(key)
fixture_env.update({
    "GIT_CONFIG_NOSYSTEM":"1", "GIT_CONFIG_GLOBAL":os.devnull,
    "GIT_CONFIG_COUNT":"2", "GIT_CONFIG_KEY_0":"core.hooksPath",
    "GIT_CONFIG_VALUE_0":os.devnull, "GIT_CONFIG_KEY_1":"commit.gpgsign",
    "GIT_CONFIG_VALUE_1":"false", "GIT_AUTHOR_NAME":"Wirelog fixture",
    "GIT_AUTHOR_EMAIL":"fixture@example.invalid", "GIT_COMMITTER_NAME":"Wirelog fixture",
    "GIT_COMMITTER_EMAIL":"fixture@example.invalid",
})

def run(args, cwd=None, check=True, **kwargs):
    kwargs.setdefault("env", fixture_env)
    return subprocess.run(args,cwd=cwd,text=True,capture_output=True,check=check,**kwargs)

with tempfile.TemporaryDirectory(prefix="wirelog-size-merge-fixture-") as temp_name:
    temp=Path(temp_name); repo=temp/"synthetic repo"; repo.mkdir()
    run(["git","init","-q","--initial-branch=main"],cwd=repo)
    run(["git","config","user.name","Wirelog fixture"],cwd=repo)
    run(["git","config","user.email","fixture@example.invalid"],cwd=repo)
    (repo/"meson_options.txt").write_text("option('tests', type: 'boolean', value: true)\noption('mbedTLS', type: 'combo', choices: ['disabled', 'enabled', 'auto'], value: 'disabled')\n")
    (repo/"meson.build").write_text("project('wirelog', 'c', version: '0.70.0')\nshared_library('wirelog', 'wirelog.c', install: false)\n")
    def write_source(nops):
        (repo/"wirelog.c").write_text(f'void fixture(void) {{ __asm__ volatile(".rept {nops}\\n nop\\n .endr"); }}\n')
    write_source(10000)
    (repo/"tests").mkdir(); (repo/"tests/baseline_size.txt").write_text("100\n")
    (repo/"tests/baseline_size.provenance.json").write_text(json.dumps({"schema_version":1,"status":"legacy-unverified","baseline_bytes":100}))
    run(["git","add","."],cwd=repo); run(["git","commit","-qm","base"],cwd=repo)
    base=run(["git","rev-parse","HEAD"],cwd=repo).stdout.strip()

    def commit_branch(name, edit):
        run(["git","checkout","-q","-b",name,base],cwd=repo)
        edit()
        run(["git","add","."],cwd=repo); run(["git","commit","-qm",name],cwd=repo)
        return run(["git","rev-parse","HEAD"],cwd=repo).stdout.strip()
    def merge_commit(pr_head):
        tree=run(["git","rev-parse",f"{pr_head}^{{tree}}"],cwd=repo).stdout.strip()
        return run(["git","commit-tree",tree,"-p",base,"-p",pr_head,"-m","synthetic PR merge"],cwd=repo).stdout.strip()
    def checkout_merge(merge):
        run(["git","update-ref","refs/heads/main",merge],cwd=repo)
        run(["git","checkout","-q","-f","main"],cwd=repo)

    pr_doc=commit_branch("pr-doc",lambda: (repo/"README.md").write_text("docs only\n"))
    merge_doc=merge_commit(pr_doc); checkout_merge(merge_doc)
    head_build=temp/"head build"; report=temp/"size report.json"
    run(["meson","setup",str(head_build),str(repo),"-Dtests=true","-DmbedTLS=disabled"])

    def compare(base_sha, pr_sha, merge_sha, expected=0):
        if report.exists(): report.unlink()
        result=run([str(orchestrator),str(head_build),base_sha,merge_sha,str(report),pr_sha],
                   cwd=repo,check=False,env={**fixture_env,"GITHUB_REPOSITORY":"fixture/wirelog"})
        if result.returncode!=expected:
            raise AssertionError(f"comparison returned {result.returncode}, expected {expected}:\n{result.stdout}\n{result.stderr}")
        return result

    compare(base,pr_doc,merge_doc,0)
    doc_report=json.loads(report.read_text())
    assert doc_report["inherited_overage"] is True
    assert doc_report["base_bytes"]==doc_report["head_bytes"]
    assert doc_report["base_sha"]==base and doc_report["head_sha"]==merge_doc

    pr_growth=commit_branch("pr-growth",lambda: write_source(10100))
    merge_growth=merge_commit(pr_growth); checkout_merge(merge_growth)
    compare(base,pr_growth,merge_growth,1)
    growth_report=json.loads(report.read_text())
    assert growth_report["status"]=="over-budget" and growth_report["head_bytes"]>growth_report["base_bytes"]
    assert growth_report["allowed_head_bytes"]==growth_report["base_bytes"]
    assert growth_report["base_sha"]==base and growth_report["head_sha"]==merge_growth

    def inflate_candidate_baseline():
        (repo/"tests/baseline_size.txt").write_text("999999999\n")
    pr_inflate=commit_branch("pr-inflate",inflate_candidate_baseline)
    merge_inflate=merge_commit(pr_inflate); checkout_merge(merge_inflate)
    inflated=compare(base,pr_inflate,merge_inflate,2)
    assert "numeric baseline update" in inflated.stderr
    assert not report.exists(), "candidate baseline inflation must fail before policy output"

    # Only a true two-parent merge with exact event base + PR head is accepted.
    run(["git","checkout","-q","pr-doc"],cwd=repo)
    result=compare(base,pr_doc,pr_doc,2)
    assert "exact two-parent" in result.stderr
    checkout_merge(merge_inflate)
    result=compare(base,pr_doc,merge_inflate,2)
    assert "second parent differs" in result.stderr
    result=compare(pr_doc,pr_inflate,merge_inflate,2)
    assert "first parent differs" in result.stderr

print("test-size-comparison: exact merge, inherited debt, growth, and baseline ownership passed")
