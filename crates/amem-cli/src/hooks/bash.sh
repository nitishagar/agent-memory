# >>> amem init (bash) >>>
if [ -n "${PS1:-}" ] && [ -z "${AMEM_INIT_DONE:-}" ]; then
AMEM_INIT_DONE=1
if [ -z "${AMEM_SESSION:-}" ]; then
  AMEM_SESSION=$(amem uuid 2>/dev/null || echo "bash-$$-$RANDOM")
  export AMEM_SESSION
fi
__amem_preexec() {
  unset AMEM_HISTORY_ID
  AMEM_HISTORY_ID=$(printf '%s' "$1" | amem history start --shell bash --cmd-from-stdin 2>/dev/null) || return 0
  export AMEM_HISTORY_ID
  AMEM_EPOCHREALTIME=${EPOCHREALTIME:-}
  export AMEM_EPOCHREALTIME
  return 0
}
__amem_precmd() {
  local ec=$?
  if [ -n "${AMEM_HISTORY_ID:-}" ]; then
    local dur_arg=""
    if [ -n "${AMEM_EPOCHREALTIME:-}" ] && [ -n "${EPOCHREALTIME:-}" ]; then
      local s_us=${AMEM_EPOCHREALTIME/./} e_us=${EPOCHREALTIME/./}
      case "$s_us$e_us" in
        *[!0-9]*) ;;
        *) local ns=$(( (10#$e_us - 10#$s_us) * 1000 )); (( ns < 0 )) && ns=0; dur_arg="--duration-ns $ns" ;;
      esac
    fi
    # shellcheck disable=SC2086
    amem history end --id "$AMEM_HISTORY_ID" --exit $ec $dur_arg 2>/dev/null || true
    unset AMEM_HISTORY_ID
  fi
  return 0
}
preexec_functions+=(__amem_preexec)
precmd_functions+=(__amem_precmd)
fi
# <<< amem init (bash) <<<
