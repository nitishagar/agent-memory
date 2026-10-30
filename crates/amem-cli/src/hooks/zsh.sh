# >>> amem init (zsh) >>>
if [[ -o interactive && -z "${AMEM_INIT_DONE:-}" ]]; then
AMEM_INIT_DONE=1
if [[ -z "${AMEM_SESSION:-}" ]]; then AMEM_SESSION=$(amem uuid 2>/dev/null || echo "zsh-$$-$RANDOM"); export AMEM_SESSION; fi
zmodload zsh/datetime 2>/dev/null || true
__amem_preexec() {
  AMEM_HISTORY_ID=$(printf '%s' "$1" | amem history start --shell zsh --cmd-from-stdin 2>/dev/null) || return 0
  export AMEM_HISTORY_ID
  AMEM_EPOCHREALTIME=$EPOCHREALTIME
  export AMEM_EPOCHREALTIME
  return 0
}
__amem_precmd() {
  local ec=$?
  if [[ -n "${AMEM_HISTORY_ID:-}" ]]; then
    local -a args=(--id "$AMEM_HISTORY_ID" --exit $ec)
    if [[ -n "${AMEM_EPOCHREALTIME:-}" && -n "${EPOCHREALTIME:-}" ]]; then
      local ns=$(( (EPOCHREALTIME - AMEM_EPOCHREALTIME) * 1000000000 ))
      (( ns < 0 )) && ns=0
      args+=(--duration-ns $ns)
    fi
    amem history end "${args[@]}" 2>/dev/null || true
    unset AMEM_HISTORY_ID
  fi
  return 0
}
autoload -Uz add-zsh-hook 2>/dev/null && { add-zsh-hook preexec __amem_preexec; add-zsh-hook precmd __amem_precmd; } || true
fi
# <<< amem init (zsh) <<<
