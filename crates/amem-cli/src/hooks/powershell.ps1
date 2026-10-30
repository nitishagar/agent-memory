# >>> amem init (powershell) >>>
# Completed-command capture via prompt override (no PSReadLine dependency):
# Get-History supplies the command line, exit code, and duration.
if (-not $env:AMEM_SESSION) {
  $env:AMEM_SESSION = (& amem uuid 2>$null)
  if (-not $env:AMEM_SESSION) { $env:AMEM_SESSION = "pwsh-$PID-$(Get-Random)" }
}
if (-not (Test-Path function:global:__amem_orig_prompt)) {
  Rename-Item function:prompt global:__amem_orig_prompt -ErrorAction SilentlyContinue
}
function global:prompt {
  $ec = $?
  try {
    $h = Get-History -Count 1 -ErrorAction SilentlyContinue
    if ($null -ne $h -and $h.Id -ne $global:__amem_last_hid) {
      $global:__amem_last_hid = $h.Id
      # ExecutionStatus (not $LASTEXITCODE, which goes stale) decides failure.
      if ($h.ExecutionStatus -eq 'Completed' -and $ec) { $code = 0 }
      elseif ($null -ne $global:LASTEXITCODE) { $code = $global:LASTEXITCODE }
      else { $code = 1 }
      $dur = 0
      if ($null -ne $h.EndExecutionTime -and $null -ne $h.StartExecutionTime) {
        $dur = [int64](($h.EndExecutionTime - $h.StartExecutionTime).Ticks * 100)
      }
      # Piped stdin: env/argv strings hit the OS MAX_ARG_STRLEN ceiling (~128KB).
      $h.CommandLine | & amem history both --shell powershell --exit $code --duration-ns $dur --cmd-from-stdin 2>$null
    }
  } catch {}
  __amem_orig_prompt
}
# <<< amem init (powershell) <<<
