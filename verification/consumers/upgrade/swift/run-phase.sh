#!/bin/sh
# Runs one upgrade phase for conformance/upgrade. Both builds open the same
# database directory, so the candidate reads the predecessor's file.
set -eu

phase=${1:?phase is required}
case "$phase" in
  predecessor)
    binary=${SYNCHRO_UPGRADE_SWIFT_PREDECESSOR:?SYNCHRO_UPGRADE_SWIFT_PREDECESSOR is required}
    version=${SYNCHRO_UPGRADE_PREDECESSOR_VERSION:?SYNCHRO_UPGRADE_PREDECESSOR_VERSION is required}
    ;;
  candidate)
    binary=${SYNCHRO_UPGRADE_SWIFT_CANDIDATE:?SYNCHRO_UPGRADE_SWIFT_CANDIDATE is required}
    version=${SYNCHRO_UPGRADE_CANDIDATE_VERSION:?SYNCHRO_UPGRADE_CANDIDATE_VERSION is required}
    ;;
  *) printf '%s\n' "unknown upgrade phase: $phase" >&2; exit 1 ;;
esac
test -x "$binary"
test -d "${SYNCHRO_UPGRADE_DATA_DIR:?SYNCHRO_UPGRADE_DATA_DIR is required}"
SYNCHRO_UPGRADE_PACKAGE_VERSION=$version exec "$binary"
