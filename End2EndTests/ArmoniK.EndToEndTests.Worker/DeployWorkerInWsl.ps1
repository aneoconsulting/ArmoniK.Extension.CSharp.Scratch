Write-Host "Deployment of the Worker on WSL. Prerequisites:"
Write-Host "    - WSL must be started."
Write-Host "    - ArmoniK must be already deployed with make deploy"
Write-Host ""

# Set current directory to the current script location
Set-Location $PSScriptRoot

$wslUser = wsl whoami
Write-Host "WSL user is $wslUser"
$wslDistrib = ((wsl --status) -replace "`0","" -split ":")[1].Trim()
Write-Host "WSL distribution is $wslDistrib"

$Project = "ArmoniK.EndToEndTests.Worker"
$Version = "1.0.0-100"
$pathToBinaries = "..\publish\$Project\$Version"

if (Test-Path $pathToBinaries)
{
	# Remove binaries from any previous build
	Remove-Item $pathToBinaries\*
}

$zipName = "$Project-v$Version.zip"
$zipPath = "..\packages\$zipName"
if (Test-Path $zipPath) {
	Write-Host "Remove existing zip file"
	Remove-Item $zipPath
}

# Trigger compilation of the worker
Write-Host "Build and publish worker"
dotnet publish --self-contained -c Release -r linux-x64 -f net8.0 .

# Find control-plane IP
$IP = $(wsl kubectl -n armonik get service ingress -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
$ingress = $(wsl kubectl -n armonik get service ingress -o json) | ConvertFrom-Json
$port = $ingress.spec.ports[-1].port
$url = "http://" + $IP + ":" + $port

# Parse appSettings.json of the client
$appSettingsPath = "..\ArmoniK.EndToEndTests.Client\appSettings.json"
try
{
	$appSettings = Get-Content $appSettingsPath -Raw | ConvertFrom-Json
	$appSettings.Grpc.EndPoint = $url
}
catch{
	Write-Error "Unexpected error (syntax error?) while parsing $appSettingsPath"
	return 1
}

# Write the control-plane url in appSettings client
Write-Host "Set control plane url $url to $appSettingsPath"
$appSettings | ConvertTo-Json -Depth 4 | Out-File $appSettingsPath
