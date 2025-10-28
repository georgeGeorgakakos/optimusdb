# 1) Ensure go.mod is correct (no bad paths like p2p/muxer/mplex)
#    If you used the correct module, you should have: github.com/libp2p/go-libp2p-mplex
#    Remove any replace/require for the OLD path.
go mod edit -droprequire  github.com/libp2p/go-libp2p/p2p/muxer/mplex 2>$null
go mod edit -dropreplace github.com/libp2p/go-libp2p/p2p/muxer/mplex 2>$null

# 2) Remove vendor (if present) so it doesn't pin old deps
if (Test-Path vendor) { Remove-Item -Recurse -Force vendor }

# 3) Remove the checksum file
if (Test-Path go.sum) { Remove-Item go.sum }

# 4) Clear the module cache (optional but useful when things got messy)
go clean -modcache

# 5) Re-hydrate deps strictly from go.mod + your imports
go mod tidy
go mod download

# 6) Sanity check: make sure no code imports the old mplex path
#git grep -n 'github.com/libp2p/go-libp2p/p2p/muxer/mplex' || echo "OK: no old mplex imports"

# 7) Build
go build ./...
