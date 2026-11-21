# Fork builds

The `Fork build` workflow builds the subscription branch using the upstream
v1.14.4 **core** release matrix: 44 build variants producing 139 packages.
It includes Linux, Windows, macOS and Android command-line binaries; the
upstream Naive/PureGo/glibc/musl variants; Windows 7 and macOS 10.13 compatibility
builds; DEB, RPM, Pacman and Alpine packages; and OpenWrt IPK/APK packages.

Build tags, architecture settings, Chromium dependencies, Go versions and
packaging commands come from `.github/workflows/build.yml`. In particular,
Naive builds use `release/DEFAULT_BUILD_TAGS`, Windows Naive builds use
`release/DEFAULT_BUILD_TAGS_WINDOWS`, and other builds use
`release/DEFAULT_BUILD_TAGS_OTHERS`. Compatibility builds retain the upstream
Go patches and tag exceptions. Do not replace these with one common tag list
or disable CGO across the entire matrix.

## Running a build

- Every push to `feat/subscription` runs the regression tests and full core matrix.
- In GitHub Actions, select **Fork build → Run workflow** for a manual build.
- Leave `version` empty to generate `1.14.4-subscription.<run>.<commit>`.
  An explicit version must retain a `-subscription.` or `-subscription-` suffix.
- APK metadata uses `1.14.4_git<run>-r0`, since APK does not accept arbitrary
  prerelease labels or commit hashes. Filenames and the executable retain the
  full subscription version. Official stable/beta/RC package versions are unchanged.
- Artifacts are retained for seven days. Each `binary-*` download contains the
  upstream-format packages for that target. The `checksums` artifact contains
  `SHA256SUMS` for all 139 packages.
- Select `publish` on a manual run to publish a prerelease after all builds,
  tests and artifact checks succeed. Existing releases are not overwritten.

The fork workflow needs no manually supplied token: GitHub provides the scoped
`GITHUB_TOKEN`. Build jobs have read access; only the optional release job has
repository write access. GPG signing is optional: set `GPG_KEY`, `GPG_KEY_ID`,
and, if applicable, `GPG_PASSPHRASE` in repository Actions secrets to use your own
key. Otherwise DEB/RPM packages are unsigned. Upstream signatures and trust
relationships cannot be inherited by a fork.

## Scope and verification

`fork-artifacts.txt` records the core filenames from the upstream
[`v1.14.4` release](https://github.com/SagerNet/sing-box/releases/tag/v1.14.4), with
the version replaced by `{version}`. `verify-fork-artifacts.py` rejects missing,
empty or unexpected files before generating checksums or allowing publication.
When updating the upstream version or matrix, update this inventory as well.

`.github/test-subscription.sh` runs race tests for subscription lifecycle,
dependency/DNS handling, public names, imports, Clash API and connection
contracts, plus tests for the locally patched AnyTLS module. It is also usable
locally with Go 1.26.10 or a compatible newer toolchain.

This workflow does not package the separate SFA/SFI/SFL/SFM/SFW graphical
clients, App Store/TestFlight releases, Docker images, or the separately
modified zashboard repository. The original upstream workflows remain in the
source tree for reference, but depend on additional publishing/signing setup.

Personal subscription URLs, DoH credentials, local JSON/YAML configurations,
runtime caches and the local `dist` directory are excluded from Git. CI uploads
only newly generated packages from its clean checkout.
