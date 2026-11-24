# Vendored Tsavorite

[Tsavorite](https://github.com/microsoft/garnet/tree/main/libs/storage/Tsavorite) is the storage
engine inside microsoft/garnet, a maintained fork of FASTER. It is not published on NuGet, so its
source is vendored here, unmodified, and compiled as `MelangeDB.Tsavorite`. `MelangeDB.Storage.Faster`
carries that assembly and the native devices inside its own package. The engine is selected with
`HotStore:PagedEngine = Tsavorite`; see [docs/CONFIGURATION.md](../../docs/CONFIGURATION.md) and
[docs/design/hot-store-bakeoff.md](../../docs/design/hot-store-bakeoff.md).

| | |
| --- | --- |
| Upstream | https://github.com/microsoft/garnet |
| Commit | `dabb3de1d9507257440be48d7a30b164f4623e6e` (2026-11-23) |
| Licence | MIT, Copyright (c) Microsoft Corporation ([LICENSE](LICENSE)) |

What is vendored:
- `libs/storage/Tsavorite/cs/src/core/**`: everything except Garnet's csproj.
- `libs/diagnostics/GarnetAsserts.cs` and `AssertMessageHandler.cs`, in `Diagnostics/`.
- The prebuilt native devices, `libs/storage/Tsavorite/cs/src/core/Device/runtimes/{linux-x64,linux-arm64,linux-musl-x64,linux-musl-arm64}/native/`.

The csproj is MelangeDB's own. It renames the assembly, drops Garnet's strong-name signing and
`InternalsVisibleTo`, and turns off nullable annotations and implicit usings, as Garnet's build has
them.

To refresh, run `tools/vendor-tsavorite.sh <commit>`. Then update the commit above, run the store
suite with `MELANGE_TEST_PAGED_ENGINE=tsavorite`, and note the refresh in the changelog.
