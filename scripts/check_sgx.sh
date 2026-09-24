#!/usr/bin/env bash
# SOTERIA SGX preflight: reports whether this machine can run SOTERIA inside
# Gramine-SGX enclaves with DCAP attestation. Read-only; needs no root.
# Supports Rocky/RHEL 9.4+ and Ubuntu 22.04/24.04.
#
# Usage: scripts/check_sgx.sh            (send the full output when asking for help)
set -uo pipefail

ok=0; warn=0; fail=0
OK()   { echo "  [ OK ] $*"; ok=$((ok + 1)); }
WARN() { echo "  [WARN] $*"; warn=$((warn + 1)); }
FAIL() { echo "  [FAIL] $*"; fail=$((fail + 1)); }
INFO() { echo "         $*"; }
section() { echo; echo "== $*"; }
have() { command -v "$1" >/dev/null 2>&1; }

pkg_installed() {
  if have rpm && rpm -q "$1" >/dev/null 2>&1; then return 0; fi
  if have dpkg-query && dpkg-query -W -f='${Status}' "$1" 2>/dev/null | grep -q "install ok installed"; then return 0; fi
  return 1
}

# ---------------------------------------------------------------- OS
section "Operating system"
OS_RELEASE=${OS_RELEASE:-/etc/os-release}
if [[ -r $OS_RELEASE ]]; then
  . "$OS_RELEASE"
  INFO "$PRETTY_NAME (ID=$ID, VERSION_ID=$VERSION_ID)"
  case "$ID" in
    rocky|rhel|almalinux|centos)
      major=${VERSION_ID%%.*}; minor=${VERSION_ID#*.}; [[ "$minor" == "$VERSION_ID" ]] && minor=0
      if (( major > 9 || (major == 9 && minor >= 4) )); then OK "EL $VERSION_ID: in-kernel SGX is supported (RHEL 9.4+)"
      elif (( major == 9 )); then FAIL "EL $VERSION_ID: in-kernel SGX needs 9.4 or later; update the OS"
      else WARN "EL $VERSION_ID: SOTERIA targets EL 9.4+"; fi ;;
    ubuntu)
      case "$VERSION_ID" in 22.04|24.04) OK "Ubuntu $VERSION_ID" ;; *) WARN "Ubuntu $VERSION_ID: tested targets are 22.04 and 24.04" ;; esac ;;
    *) WARN "untested distribution: $ID" ;;
  esac
else
  WARN "$OS_RELEASE not found"
fi
INFO "kernel $(uname -r), $(nproc) CPUs, $(awk '/MemTotal/ {printf "%.1f GiB RAM", $2/1048576}' /proc/meminfo)"

# ---------------------------------------------------------------- CPU and driver
section "SGX hardware and kernel driver"
flags=$(grep -m1 '^flags' /proc/cpuinfo)
if grep -qw sgx <<<"$flags"; then OK "CPU exposes SGX"; else FAIL "CPU flag 'sgx' missing (not supported, or disabled in BIOS)"; fi
if grep -qw sgx_lc <<<"$flags"; then OK "Flexible Launch Control (sgx_lc) present"; else FAIL "no Flexible Launch Control (sgx_lc): needed by the in-kernel driver and DCAP"; fi

for dev in /dev/sgx_enclave /dev/sgx_provision; do
  if [[ -e $dev ]]; then
    OK "$dev exists ($(stat -c '%U:%G %a' "$dev"))"
    [[ -r $dev && -w $dev ]] || WARN "$dev is not read/writable by $(id -un); add the user to group $(stat -c %G "$dev")"
  else
    FAIL "$dev missing: in-kernel SGX driver not loaded (check BIOS SGX setting and kernel version)"
  fi
done
[[ -e /dev/sgx_vepc ]] && INFO "/dev/sgx_vepc present (host can also give SGX to VMs)"
[[ -e /dev/isgx ]] && WARN "/dev/isgx present: legacy out-of-tree driver loaded; remove it and use the in-kernel driver"
INFO "groups of $(id -un): $(id -Gn)"

epc=0
for f in /sys/devices/system/node/node*/x86/sgx_total_bytes; do
  [[ -r $f ]] && epc=$((epc + $(cat "$f")))
done
if (( epc > 0 )); then INFO "EPC size: $((epc / 1048576)) MiB"
else INFO "EPC size not exposed in sysfs on this kernel (see: sudo dmesg | grep -i sgx)"; fi

# ---------------------------------------------------------------- Intel SGX software
section "Intel SGX PSW / DCAP packages"
for p in libsgx-urts sgx-aesm-service libsgx-dcap-ql libsgx-dcap-default-qpl libsgx-dcap-quote-verify; do
  if pkg_installed "$p"; then OK "$p installed"; else FAIL "$p not installed"; fi
done
for p in libsgx-enclave-common libsgx-ae-qe3 libsgx-ae-pce libsgx-aesm-ecdsa-plugin libsgx-aesm-pce-plugin; do
  pkg_installed "$p" && INFO "$p installed" || INFO "$p not installed (usually pulled in as a dependency)"
done

if have systemctl && [[ -d /run/systemd/system ]]; then
  if systemctl is-active --quiet aesmd 2>/dev/null; then OK "aesmd service is active"
  else FAIL "aesmd service is not active (systemctl status aesmd)"; fi
else
  WARN "systemd not running: cannot check the aesmd service"
fi

# ---------------------------------------------------------------- PCCS / QPL
section "DCAP collateral (PCCS via the quote provider library)"
qcnl=/etc/sgx_default_qcnl.conf
if [[ -r $qcnl ]]; then
  OK "$qcnl found"
  pccs=$(grep -oE '"pccs_url"[[:space:]]*:[[:space:]]*"[^"]+"' "$qcnl" | sed -E 's/.*"([^"]+)"$/\1/' | head -1)
  [[ -z $pccs ]] && pccs=$(grep -E '^[[:space:]]*PCCS_URL[[:space:]]*=' "$qcnl" | cut -d= -f2- | tr -d ' ' | head -1)
  if [[ -n $pccs ]]; then
    INFO "pccs_url: $pccs"
    if have curl; then
      code=$(curl -sk -o /dev/null -w '%{http_code}' --max-time 10 "${pccs%/}/rootcacrl" || echo 000)
      case "$code" in
        200) OK "PCCS answered ${pccs%/}/rootcacrl (HTTP 200)" ;;
        000) FAIL "PCCS not reachable at $pccs" ;;
        *)   WARN "PCCS returned HTTP $code for ${pccs%/}/rootcacrl" ;;
      esac
    fi
  else
    WARN "no pccs_url in $qcnl"
  fi
  grep -qE '"use_secure_cert"[[:space:]]*:[[:space:]]*false' "$qcnl" && WARN "use_secure_cert is false (fine for a local self-signed PCCS, not for production)"
else
  FAIL "$qcnl missing (installed by libsgx-dcap-default-qpl)"
fi

# ---------------------------------------------------------------- Gramine
section "Gramine"
if have gramine-sgx; then
  OK "gramine-sgx found: $(command -v gramine-sgx)"
  have gramine-manifest && INFO "$(gramine-manifest --version 2>/dev/null | head -1)"
else
  FAIL "gramine-sgx not found"
fi
if have is-sgx-available; then
  out=$(is-sgx-available 2>&1); rc=$?
  echo "$out" | sed 's/^/         /'
  if (( rc == 0 )); then OK "is-sgx-available: SGX usable"; else FAIL "is-sgx-available reported a problem (exit $rc)"; fi
  if grep -qiE 'SGX2[^:]*:[[:space:]]*true' <<<"$out"; then OK "SGX2 (needed for EDMM) supported"
  else WARN "SGX2/EDMM not reported; enclaves must be sized statically (no sgx.edmm_enable)"; fi
else
  WARN "is-sgx-available not found (ships with Gramine)"
fi
key="${XDG_CONFIG_HOME:-$HOME/.config}/gramine/enclave-key.pem"
if [[ -r $key ]]; then OK "enclave signing key: $key"; else WARN "no enclave signing key yet (create with: gramine-sgx-gen-private-key)"; fi

# ---------------------------------------------------------------- Java / build tools
section "Java and build tools"
if have java; then
  v=$(java -version 2>&1 | grep -m1 ' version ')
  INFO "java: $v ($(readlink -f "$(command -v java)"))"
  if grep -qE '"17\.' <<<"$v"; then OK "Java 17"; else WARN "SOTERIA targets Java 17"; fi
else
  FAIL "java not found (Rocky: dnf install java-17-openjdk-devel)"
fi
for d in /usr/lib/jvm/java-17-openjdk /usr/lib/jvm/java-17-openjdk-amd64; do
  [[ -d $d ]] && INFO "JDK 17 home: $d"
done
have sbt && INFO "sbt: $(command -v sbt)" || WARN "sbt not found (needed to build SOTERIA)"
have make && INFO "make: $(command -v make)" || WARN "make not found"

# ---------------------------------------------------------------- SELinux
section "SELinux"
if have getenforce; then
  mode=$(getenforce)
  case "$mode" in
    Enforcing)  WARN "SELinux is Enforcing: if enclaves fail to start, test once with 'sudo setenforce 0'; if that fixes it, we write a targeted policy" ;;
    Permissive) OK "SELinux is Permissive (denials are logged, not enforced)" ;;
    *)          OK "SELinux is $mode" ;;
  esac
else
  INFO "SELinux tools not present"
fi

# ---------------------------------------------------------------- summary
section "Summary"
echo "  $ok OK, $warn warnings, $fail failures"
(( fail == 0 )) && echo "  Ready for the Gramine bring-up." || echo "  Fix the failures above first (scripts/install_sgx.sh and scripts/install_gramine.sh will cover them)."
exit 0
