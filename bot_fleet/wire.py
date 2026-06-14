"""Binary wire codec for the HFT-Arena order protocol (WIRE=binary).

Fixed-layout, little-endian, no padding — sent as WebSocket *binary* frames. This
removes JSON parse/serialize from the hot path, so the benchmark measures the
matching engine rather than the JSON parser (closer to real HFT binary protocols).

Layout (must match every reference engine):
  REQUEST  (bot -> engine), 26 bytes:  B type, B side, Q id, i px, i qty, Q target
                                        type: 1=limit 2=market 3=cancel; side: 0=buy 1=sell
  ACK      (engine -> bot), 17 bytes:   B 1, Q id, Q ts
  FILL     (engine -> bot), 25 bytes:   B 2, Q id, i px, i qty, Q maker
"""
import struct

REQ = struct.Struct("<BBQiiQ")      # 26 bytes
ACK = struct.Struct("<BQQ")         # 17 bytes
FILL = struct.Struct("<BQiiQ")      # 25 bytes

LIMIT, MARKET, CANCEL = 1, 2, 3
T_ACK, T_FILL = 1, 2


def enc_limit(oid, buy, px, qty):
    return REQ.pack(LIMIT, 0 if buy else 1, oid, px, qty, 0)


def enc_market(oid, buy, qty):
    return REQ.pack(MARKET, 0 if buy else 1, oid, 0, qty, 0)


def enc_cancel(oid, target):
    return REQ.pack(CANCEL, 0, oid, 0, 0, target)


def reply_type(data: bytes) -> int:
    return data[0]


def dec_ack(data: bytes):
    _, oid, ts = ACK.unpack(data)
    return oid, ts


def dec_fill(data: bytes):
    _, oid, px, qty, maker = FILL.unpack(data)
    return oid, px, qty, maker
