cd C:\Users\georg\GolandProjects\optimusdb-lsa

# 1. Edit go.mod - remove line 7
# (Delete: github.com/go-skynet/go-llama.cpp v0.0.0-20240314183750-6a8041ef6b46)

# 2. Disable the package
Rename-Item contextualMetadata contextualMetadata.DISABLED

# 3. Clean up
go mod tidy
rm -rf vendor
go mod vendor

# 4. Build
docker build --no-cache -t optimusdb .