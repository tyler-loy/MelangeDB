#!/usr/bin/env bash
# Refreshes src/MelangeDB.Tsavorite from microsoft/garnet at a given commit; see its README.md.
set -euo pipefail
commit="${1:?usage: tools/vendor-tsavorite.sh <garnet commit>}"
root="$(cd "$(dirname "$0")/.." && pwd)"
dest="$root/src/MelangeDB.Tsavorite"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

git -C "$work" init -q garnet
git -C "$work/garnet" remote add origin https://github.com/microsoft/garnet.git
git -C "$work/garnet" sparse-checkout set libs/storage/Tsavorite/cs/src/core libs/diagnostics LICENSE
git -C "$work/garnet" fetch -q --depth 1 origin "$commit"
git -C "$work/garnet" checkout -q FETCH_HEAD

core="$work/garnet/libs/storage/Tsavorite/cs/src/core"
find "$dest" -mindepth 1 -maxdepth 1 ! -name 'MelangeDB.Tsavorite.csproj' ! -name 'README.md' -exec rm -rf {} +
(cd "$core" && tar --exclude='./Tsavorite.core.csproj' --exclude='./bin' --exclude='./obj' -cf - .) | (cd "$dest" && tar -xf -)
mkdir -p "$dest/Diagnostics"
cp "$work/garnet/libs/diagnostics/GarnetAsserts.cs" "$work/garnet/libs/diagnostics/AssertMessageHandler.cs" "$dest/Diagnostics/"
cp "$work/garnet/LICENSE" "$dest/LICENSE"
# Keep only the Linux native devices MelangeDB ships (Windows uses the managed LocalStorageDevice).
find "$dest/Device/runtimes" -mindepth 1 -maxdepth 1 ! -name 'linux-*' -exec rm -rf {} +
echo "Vendored Tsavorite at $commit into $dest; update the commit in its README.md."
