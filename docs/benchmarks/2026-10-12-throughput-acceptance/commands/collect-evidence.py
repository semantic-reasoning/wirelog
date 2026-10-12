from pathlib import Path
import hashlib,json,re,shutil,statistics
root=Path("/srv/home/joykim/git/wirelog/docs/benchmarks/2026-10-12-throughput-acceptance")
controlled=Path("/home/joykim/.cache/wirelog-1913-controlled-69111650")
plan=json.loads((controlled/"predeclared-plan.json").read_text())
selected=["testlog.txt","host.txt","gate-before.txt","gate-after.txt","meson-config.txt","compilers.json","image-identity.txt","gcc-version.txt","meson-version.txt","ninja-version.txt","uv-version.txt","sha256.txt","campaign-status.txt","qualification-status.txt","telemetry-status.txt","meson-test.exit","configure.log","meson-test.stdout","meson-test.stderr","cooldown.txt","measured-source.sha256","source-head.txt","source-dirty.txt","source-integrity-after.txt","wrapper.exit","started-utc.txt","finished-utc.txt"]
blocks=[]
for block in plan["order"]:
 src=controlled/"timings"/block
 dst=root/"raw"/block;dst.mkdir(parents=True,exist_ok=True)
 assert (src/"finished-utc.txt").exists(),block+" incomplete"
 for name in selected: shutil.copyfile(src/name,dst/name)
 if (src/"intro-buildoptions.json").exists(): shutil.copyfile(src/"intro-buildoptions.json",dst/"intro-buildoptions.json")
 variant=block.rsplit("-",1)[0]
 identity={"base_commit":plan["base_commit"],"measured_source_tree":plan["candidate_code_tree"] if variant=="candidate" else plan["base_tree"],"source_head_tree_is_measured_tree":variant=="base","candidate_patch_sha256":plan["code_diff_sha256"] if variant=="candidate" else None,"note":"HEAD is the unmodified base commit; candidate source contains the recorded two-path working-tree patch. measured-source.sha256 and post-run integrity check identify actual compiled source."}
 (dst/"source-identity.json").write_text(json.dumps(identity,indent=2)+"\n")
 blocks.append({"id":block,"origin":"local strict wrapper on atlas-perf","source_identity":identity})
for run,artifact in [("38195651787","11723561762"),("38195709101","11722869434")]:
 blocks.append({"id":"main-"+run,"origin":"GitHub Actions strict Linux acquisition","run_url":"https://github.com/semantic-reasoning/wirelog/actions/runs/"+run,"artifact_url":"https://github.com/semantic-reasoning/wirelog/actions/runs/"+run+"/artifacts/"+artifact,"source_identity":{"base_commit":plan["base_commit"],"measured_source_tree":plan["base_tree"]}})
controlled_blocks=blocks[:4]
reference=controlled/"timings/base-1"
for block in controlled_blocks:
 d=controlled/"timings"/block["id"]
 for name in ["image-identity.txt","gcc-version.txt","meson-version.txt","ninja-version.txt","uv-version.txt","compilers.json"]:
  assert (d/name).read_bytes()==(reference/name).read_bytes(),(block["id"],name)
 assert sorted((d/"meson-config.txt").read_text().splitlines())==sorted((reference/"meson-config.txt").read_text().splitlines()),block["id"]
 def hashes(p): return {line.split()[1]:line.split()[0] for line in (p/"sha256.txt").read_text().splitlines()}
 hd,hr=hashes(d),hashes(reference)
 assert {k:v for k,v in hd.items() if k.startswith("bench/")}=={k:v for k,v in hr.items() if k.startswith("bench/")}
for b in blocks:
 d=root/"raw"/b["id"]
 log=(d/"testlog.txt").read_text()
 b["status"]={k:v for k,v in re.findall(r"^(\w+)=(.+)$",(d/"campaign-status.txt").read_text(),re.M)}
 b["statistics"]={}
 for name,prefix,target in [("CRDT","test_crdt_perf_gate",38120),("CSPA","test_cspa_perf_gate",2050)]:
  m=re.search(prefix+r": raw_ms = ([^\n]+).*?stdev_ms\s*= ([0-9.]+) \(CoV ([0-9.]+)%\).*?median_ms\s*= ([0-9.]+) \(target ([0-9]+)\)",log,re.S)
  assert m,(b["id"],name)
  samples=list(map(float,m[1].split()));assert len(samples)==9
  sd=statistics.pstdev(samples);cov=sd/statistics.mean(samples)*100
  assert abs(float(m[2])-sd)<0.06 and abs(float(m[3])-cov)<0.006
  assert float(m[4])==statistics.median(samples) and int(m[5])==target
  b["statistics"][name]={"raw_ms":samples,"median_ms":float(m[4]),"reported_cov_percent":float(m[3]),"recomputed_cov_percent_from_rounded_samples":cov,"target_ms":target}
 assert "result     = 104851 (expected 104851)" in log
 assert "tuples     = 20381/20,381" in log and "iterations = 6/6" in log
 b["files"]={str(p.relative_to(d)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(d.iterdir()) if p.is_file()}
manifest={"scope":"All four predeclared controlled strict blocks, two same-source current-main strict acquisitions, separate native diagnostic profiles and failed initial profile launches. Raw selected files copied byte-for-byte; generated source identity and statistics are labeled separately.","predeclared_plan":plan,"blocks":blocks,"all_file_sha256":{str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(root.rglob("*")) if p.is_file() and p.name not in ["manifest.json","README.md"]}}
(root/"manifest.json").write_text(json.dumps(manifest,indent=2)+"\n")
for b in blocks: print(b["id"],b["status"],b["statistics"])
