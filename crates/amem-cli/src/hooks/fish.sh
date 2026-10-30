# >>> amem init (fish) >>>
if status is-interactive
  if test -z "$AMEM_INIT_DONE"
    set -g AMEM_INIT_DONE 1
    if test -z "$AMEM_SESSION"; set -gx AMEM_SESSION (amem uuid 2>/dev/null; or echo "fish-$fish_pid-$RANDOM"); end
    function __amem_preexec --on-event fish_preexec
      if set -q fish_private_mode; return 0; end
      set -g AMEM_HISTORY_ID (printf '%s' "$argv[1]" | amem history start --shell fish --cmd-from-stdin 2>/dev/null); or return 0
    end
    function __amem_postexec --on-event fish_postexec
      set -l ec $status
      if set -q AMEM_HISTORY_ID[1]
        amem history end --id "$AMEM_HISTORY_ID" --exit $ec 2>/dev/null; or true
        set -e AMEM_HISTORY_ID
      end
    end
  end
end
# <<< amem init (fish) <<<
