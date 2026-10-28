"""Estimate the local clock's offset with a single SNTP query (RFC 4330).

Any latency computed as "our clock minus Wikimedia's clock" contains this offset.
Phase 0 found the WSL2 clock several seconds behind, so the producer measures and
exports it instead of assuming clocks agree.
"""

import socket
import struct
import time

NTP_TO_UNIX_EPOCH_S = 2_208_988_800  # 1900-01-01 to 1970-01-01


def _ntp_timestamp(raw: bytes) -> float:
    seconds, fraction = struct.unpack("!II", raw)
    return seconds - NTP_TO_UNIX_EPOCH_S + fraction / 2**32


def sntp_offset_seconds(server: str = "pool.ntp.org", timeout_s: float = 2.0) -> float | None:
    """Return (server time - local time) in seconds, or None if the query fails.

    Positive means the local clock is behind. Accuracy is roughly half the network
    round trip (tens of milliseconds), plenty to judge a seconds-scale skew.
    """
    request = b"\x23" + 47 * b"\0"  # LI=0, version 4, mode 3 (client)
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.settimeout(timeout_s)
            t1 = time.time()
            sock.sendto(request, (server, 123))
            response, _ = sock.recvfrom(48)
            t4 = time.time()
    except OSError:
        return None
    if len(response) < 48:
        return None
    t2 = _ntp_timestamp(response[32:40])  # server receive time
    t3 = _ntp_timestamp(response[40:48])  # server transmit time
    return ((t2 - t1) + (t3 - t4)) / 2
