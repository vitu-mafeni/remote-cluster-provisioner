
shopt -s nullglob
list_maps() {   # prints: name sd vendor model
  for dm in /sys/block/dm-*; do
    [[ $(cat "$dm/dm/uuid" 2>/dev/null) == mpath-* ]] || continue
    for s in "$dm"/slaves/*; do
      sd=$(basename "$s")
      echo "$(cat "$dm/dm/name") $sd $(tr -d ' ' < /sys/block/$sd/device/vendor 2>/dev/null) $(tr -d ' ' < /sys/block/$sd/device/model 2>/dev/null)"
    done
  done
}

echo "== 1. Checking what multipath is managing =="
foreign=0
while read -r name sd v m; do
  if [[ $v == IET && $m == VIRTUAL-DISK ]]; then
    echo "  $name <- /dev/$sd ($v $m)  Longhorn, safe to release"
  else
    echo "  $name <- /dev/$sd ($v $m)  NOT Longhorn"; foreign=1
  fi
done < <(list_maps)
[[ -z $(list_maps) ]] && echo "  no multipath maps present"
if (( foreign )); then
  echo "ABORT: real multipath devices are in use on $(hostname). Keep multipathd and rely on the blacklist instead."
  exit 1
fi

echo "== 2. Disabling multipathd =="
systemctl disable --now multipathd.service multipathd.socket 2>&1 | sed 's/^/  /'
multipath -F >/dev/null 2>&1

echo "== 3. Verifying =="
ok=1
for u in multipathd.service multipathd.socket; do
  a=$(systemctl is-active $u); e=$(systemctl is-enabled $u 2>/dev/null)
  if [[ $a != active && $e != enabled ]]; then echo "  OK   $u ($a, $e)"
  else echo "  FAIL $u ($a, $e)"; ok=0; fi
done
left=$(list_maps)
if [[ -z $left ]]; then echo "  OK   no multipath maps remain"
else echo "  FAIL maps still present (device busy?):"; echo "$left" | sed 's/^/       /'; ok=0; fi

(( ok )) && echo "ALL GOOD on $(hostname): multipathd disabled, Longhorn disks released." \
         || { echo "SOMETHING IS WRONG on $(hostname), see above."; exit 1; }
