# >>> amem init (fish) >>>
if status is-interactive
  if test -z "$AMEM_INIT_DONE"
    set -g AMEM_INIT_DONE 1
    if test -z "$AMEM_SESSION"; set -gx AMEM_SESSION (amem uuid 2>/dev/null; or echo "fish-$fish_pid-$RANDOM"); end
    function __amem_preexec --on-event fish_preexec
      if set -q fish_private_mode; return 0; end
      set -e AMEM_HISTORY_ID
      set -g AMEM_HISTORY_ID (printf '%s' "$argv[1]" | amem history start --shell fish --cmd-from-stdin 2>/dev/null); or return 0
    end
    function __amem_postexec --on-event fish_postexec
      set -l ec $status
      if set -q AMEM_HISTORY_ID[1]
        # CMD_DURATION is ms since the last command finished; missing on old fish.
        set -l dur_args
        if set -q CMD_DURATION[1]; set dur_args --duration-ns (math "$CMD_DURATION * 1000000" 2>/dev/null); end
        amem history end --id "$AMEM_HISTORY_ID" --exit $ec $dur_args 2>/dev/null; or true
        set -e AMEM_HISTORY_ID
      end
    end
  end
end
# <<< amem init (fish) <<<
