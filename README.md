# kache

This program caches Rust build products in s3. It is intended to be used with GitHub Actions.

## Design

In your GitHub Action build pipeline, before your build:

Install kache from github releases:

```bash
set -euxo pipefail

CARGO_BIN_DIR="$(dirname "$(which cargo)")"
TARGET_ARCH="$(rustc --version --verbose | sed -n 's/host: //p')"

curl \
    --user-agent "github actions build script for $GITHUB_REPOSITORY" \
    --location \
    --silent \
    --show-error \
    --fail \
    "https://github.com/cargo-quick/kache/releases/download/v0.1.2/kache-v0.1.2-${TARGET_ARCH}.tar.gz" \
    | tar -xzvvf - -C "$CARGO_BIN_DIR"
```

Load from the s3 cache:

```yaml
  - run: kache load "${{ runner.os }}-${{ hashFiles('**/Cargo.lock') }}-${{ github.head_ref }}" "${{ runner.os }}-${{ hashFiles('**/Cargo.lock') }}" "${{ runner.os }}"
    env:
        AWS_ACCESS_KEY_ID: ${{ secrets.BUILD_ACCESS_KEY_ID }}
        AWS_SECRET_ACCESS_KEY: ${{ secrets.BUILD_SECRET_ACCESS_KEY }}
        AWS_REGION: us-east-1
        KACHE_BUCKET: your-s3-bucket
```

This will attempt to find a cache that matches your branch and lockfiles, but fall back to
using the cache for your lockfiles, or just your OS if none is available.

After your build pipeline, run something like this, with the same environment variables set:

```bash
kache save "${{ runner.os }}-${{ hashFiles('**/Cargo.lock') }}-${{ github.head_ref }}"
```

This will save a cache for your branch, based on top of the cache that you pulled from.

Periodically, you will probably want to run a build that doesn't pull from any caches, and pushes to `${{ runner.os }}-${{ hashFiles('**/Cargo.lock') }}` and `${{ runner.os }}`.

## Caveats

* We still don't know how to cache rocksdb, so this will still be rebuilt every time.
* When re-saving the cache, we only upload files that have changed (mtime) since unpacking the previous cache.
* This program will never delete any files. It will happily cache files that are built from crates in your local repo, so the cache will continue to grow as those files churn.
* We currently keep layering tarballs on top of tarballs forever. We should probably find a way to avoid doing that.
* We currently only do single-threaded compression. Patches welcome.

## Releasing

```
git tag v0.1.0 -m v0.1.0
git push --tags
```

Fingers crossed, github actions will do the rest.
