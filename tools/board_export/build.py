"""Build a Getworkin bridge inside a pinned, unmodified FreeHire source tree.

The upstream repository's internal imports require building inside its module.
Only our command is overlaid; upstream adapters, transport and dictionaries stay intact.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parent


def build(upstream=None, go="go", test=False):
    pin = json.loads((ROOT / "upstream.json").read_text())
    cache = ROOT / ".cache"
    cache.mkdir(exist_ok=True)
    source = cache / "freehire"
    if not source.exists():
        with tempfile.TemporaryDirectory(dir=cache) as folder:
            staged_source = Path(folder) / "freehire"
            staged_source.mkdir()
            if upstream:
                # Archive exactly the pinned tree; never modify the user's upstream checkout.
                archive = subprocess.Popen(["git", "-C", str(upstream), "archive", pin["commit"]], stdout=subprocess.PIPE)
                try:
                    subprocess.run(["tar", "-xf", "-", "-C", str(staged_source)], stdin=archive.stdout, check=True)
                finally:
                    archive.stdout.close()
                    archive_code = archive.wait()
                if archive_code != 0:
                    raise RuntimeError("Could not archive the pinned upstream commit")
            else:
                subprocess.run(["git", "init", str(staged_source)], check=True)
                subprocess.run(["git", "-C", str(staged_source), "remote", "add", "origin", pin["repository"]], check=True)
                subprocess.run(["git", "-C", str(staged_source), "fetch", "--depth=1", "origin", pin["commit"]], check=True)
                subprocess.run(["git", "-C", str(staged_source), "checkout", "--detach", "FETCH_HEAD"], check=True)
            (staged_source / ".getworkin-pin").write_text(pin["commit"])
            staged_source.rename(source)
    if not (source / ".getworkin-pin").exists() or (source / ".getworkin-pin").read_text() != pin["commit"]:
        raise RuntimeError("Upstream cache is incomplete or on a different pin; move .cache/freehire aside and retry")
    command = source / "cmd" / "getworkin"
    command.mkdir(exist_ok=True)
    for path in (ROOT / "bridge").glob("*.go"):
        shutil.copy2(path, command / path.name)
    env = dict(os.environ, GOCACHE=str(cache / "go-build"), GOPATH=str(cache / "go"))
    if test:
        subprocess.run([go, "test", "./cmd/getworkin"], cwd=source, env=env, check=True)
        subprocess.run([go, "vet", "./cmd/getworkin"], cwd=source, env=env, check=True)
        subprocess.run([go, "test", "./cmd/harvest-ats"], cwd=source, env=env, check=True)
    target = ROOT / "bin" / "getworkin-ingest"
    target.parent.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(dir=target.parent) as folder:
        staged = Path(folder) / target.name
        subprocess.run([go, "build", "-trimpath", "-ldflags", "-X=main.upstreamCommit=" + pin["commit"],
                        "-o", str(staged), "./cmd/getworkin"], cwd=source, env=env, check=True)
        os.replace(staged, target)
    shutil.copy2(source / "LICENSE", target.parent / "FREEHIRE_LICENSE")
    subprocess.run([go, "build", "-trimpath", "-o", str(target.parent / "freehire-discover-careers"),
                    "./cmd/harvest-ats"], cwd=source, env=env, check=True)
    print(target)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", type=Path, help="optional local FreeHire git checkout containing the pinned commit")
    parser.add_argument("--go", default="go")
    parser.add_argument("--test", action="store_true")
    args = parser.parse_args()
    build(args.upstream, args.go, args.test)
