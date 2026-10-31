# Wheel release and recovery

`build_wheels.yml` publishes on version-tag pushes only after all platform
builds, the complete wheel-test matrix and strict Twine metadata checks pass.
The workflow-level `PYREX_VERSION` must match the intended tag. macOS wheels
target macOS 14 but build/test on the supported macOS 15 arm64 runner.

If a runner outage blocks testing after successful builds, do not move the tag
or bypass tests. Dispatch the current workflow with the existing tag and run:

```sh
gh workflow run build_wheels.yml --repo mylonasc/pyrex --ref main \
  -f release_tag=v0.7.1 -f reuse_run_id=41202891729
```

Recovery rejects mismatched versions, malformed run IDs and source runs that
are not completed tag-push executions of this repository's wheel workflow.
The source run must match the tag SHA/name and have all three wheel builds
successful. Downloads use the original run ID; every test checks out the
original source SHA. All wheel tests run again, including macOS, before
publication. The release tag is never rewritten. Trusted Publishing is still
performed by `build_wheels.yml`; the recovery workflow itself runs on `main`.

With `release_tag` but no `reuse_run_id`, the workflow rebuilds that exact
tagged source and applies the same test/publish gates. With no release tag,
manual runs on branches build/test only; tag refs retain the normal release
behavior. PyPI versions are immutable: verify release availability before
starting recovery, and never republish a different wheel under the same name.
