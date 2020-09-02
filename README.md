Modeling Tool

A tool set for modeling training.

This is not an officially supported Google product.
# Using with Bazel
1. Compile and run the binary 
   ```bash
   bazel build modeling-tool
   bazel run modeling-tool [GCP PROJECT] [CLOUD SPANNER INSTANCE] [CLOUD SPANNER DATABASE]
   ```
   dry-run mode:
   ```bash
   bazel run modeling-tool [GCP PROJECT] [CLOUD SPANNER INSTANCE] [CLOUD SPANNER DATABASE] --dry-run
   ```
2. Compile and run the unit test
   ```bash
   bazel build modeling-tool-test
   bazel run modeling-tool-test
   ```

