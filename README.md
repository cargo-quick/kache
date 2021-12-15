# kache

This program caches Rust build products in s3. It is intended to be used with GitHub Actions.

## Design

In your GitHub Action build pipeline, before your build, run:

```bash
cargo install cargo-quickinstall
cargo quickinstall kache
kache load "${{ runner.os }}-${{ hashFiles('**/Cargo.lock') }}-${{ github.head_ref }}" "${{ runner.os }}-${{ hashFiles('**/Cargo.lock') }}" "${{ runner.os }}"
```

This will attempt to find a cache that matches your branch and lockfiles, but fall back to
using the cache for your lockfiles, or just your OS if none is available.

After your build pipeline, run:

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
