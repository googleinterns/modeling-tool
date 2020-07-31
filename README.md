Modeling Tool

A tool set for modeling training.

This is not an officially supported Google product.

# Using with CMake
3. Configure CMake
   
   ```bash
   cd $HOME/modeling-tool
   cmake -H. -B.build -DCMAKE_TOOLCHAIN_FILE=$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake
   cmake --build .build
   ```
4. Run the executable
   ```bash
   .build/modeling-tool [GCP PROJECT] [CLOUD SPANNER INSTANCE] [CLOUD SPANNER DATABASE]
   ```

