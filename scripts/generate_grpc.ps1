$ErrorActionPreference = "Stop"

$root = Resolve-Path (Join-Path $PSScriptRoot "..")
$protoDir = Join-Path $root "proto"
$outDir = Join-Path $root "shared\grpc_generated"

New-Item -ItemType Directory -Force -Path $outDir | Out-Null

& c:\Github\.venv\Scripts\python.exe -m grpc_tools.protoc `
  -I $protoDir `
  --python_out=$outDir `
  --grpc_python_out=$outDir `
  (Join-Path $protoDir "inventory.proto")

if ($LASTEXITCODE -ne 0) {
  throw "gRPC code generation failed"
}

Write-Host "Generated gRPC stubs in $outDir"
