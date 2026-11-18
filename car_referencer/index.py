import mmap
import struct
from itertools import chain

import dag_cbor
import numpy as np
import pandas as pd
import tqdm
from multiformats import CID

# CARv2 files start with this fixed pragma: varint(10) + dag-cbor {"version": 2}
CARV2_PRAGMA = bytes.fromhex("0aa16776657273696f6e02")
CARV2_HEADER_SIZE = 40  # characteristics (16) + data offset, size, index offset
INDEX_SORTED = 0x0400
MULTIHASH_INDEX_SORTED = 0x0401


def _uvarint(buf, pos):
    """Decode an unsigned varint in ``buf`` at ``pos``.

    Returns the value and the position after the varint.
    """
    value = shift = 0
    while True:
        byte = buf[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, pos
        shift += 7


def _cid_end(buf, pos):
    """Return the position after the binary CID that starts at ``pos``."""
    if buf[pos] == 0x12 and buf[pos + 1] == 0x20:  # CIDv0
        return pos + 34
    version, pos = _uvarint(buf, pos)
    if version != 1:
        raise ValueError(f"CIDv{version} is not supported")
    _, pos = _uvarint(buf, pos)  # codec
    _, pos = _uvarint(buf, pos)  # multihash code
    digest_size, pos = _uvarint(buf, pos)
    return pos + digest_size


def _read_section(buf, pos):
    """Read the section header at ``pos`` without reading the payload.

    Returns the CID, the payload offset, the payload size and the position
    of the next section.
    """
    length, start = _uvarint(buf, pos)
    payload_offset = _cid_end(buf, start)
    cid = CID.decode(bytes(buf[start:payload_offset]))
    end = start + length
    return cid, payload_offset, end - payload_offset, end


def _read_multiwidth_index(buf, pos):
    """Read the section offsets of a go-car ``IndexSorted`` index body."""
    offsets = []
    (n_buckets,) = struct.unpack_from("<i", buf, pos)
    pos += 4
    for _ in range(n_buckets):
        width, length = struct.unpack_from("<Iq", buf, pos)
        pos += 12
        # each entry consists of the digest followed by a uint64 offset
        offsets.extend(
            struct.unpack_from("<Q", buf, entry + width - 8)[0]
            for entry in range(pos, pos + length, width)
        )
        pos += length
    return offsets, pos


def _read_carv2_index(buf, pos):
    """Read the section offsets (relative to the data payload) of a CARv2
    index. Returns ``None`` if the index format is not supported."""
    codec, pos = _uvarint(buf, pos)
    if codec == INDEX_SORTED:
        offsets, _ = _read_multiwidth_index(buf, pos)
        return offsets
    if codec == MULTIHASH_INDEX_SORTED:
        offsets = []
        (n_codes,) = struct.unpack_from("<i", buf, pos)
        pos += 4
        for _ in range(n_codes):
            pos += 8  # multihash code
            code_offsets, pos = _read_multiwidth_index(buf, pos)
            offsets.extend(code_offsets)
        return offsets
    return None


def _read_carv1_header(buf, pos, carfile):
    """Check the CARv1 header at ``pos`` and return the first section offset."""
    header_size, pos = _uvarint(buf, pos)
    header = dag_cbor.decode(bytes(buf[pos : pos + header_size]))
    if not isinstance(header, dict) or header.get("version") != 1:
        raise ValueError(f"{carfile} does not contain a valid CARv1 header")
    return pos + header_size


def _scan_sections(buf, pos, end):
    """Yield the sections between ``pos`` and ``end`` of a CARv1 payload."""
    while pos < end:
        if buf[pos] == 0:  # zero-length section, treated as end of data
            return
        cid, payload_offset, size, pos = _read_section(buf, pos)
        yield cid, payload_offset, size


def _blocks(carfile, buf, sections, verify):
    for cid, payload_offset, size in sections:
        if verify:
            data = buf[payload_offset : payload_offset + size]
            if cid.hashfun.digest(data) != cid.digest:
                raise ValueError(
                    f"{carfile} is corrupted. Block '{cid}' could not be verified"
                )
        yield cid.digest, carfile, payload_offset, size


def _mmap(carfile):
    with open(carfile, "rb") as f:
        return mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)


def read_carv1(carfile, verify=False):
    """Yield ``(digest, file, offset, size)`` for each block in a CARv1 file.

    Only the section headers are read. With ``verify=True`` the block data is
    read as well and checked against the CID.
    """
    buf = _mmap(carfile)
    try:
        first_section = _read_carv1_header(buf, 0, carfile)
        sections = _scan_sections(buf, first_section, len(buf))
        yield from _blocks(carfile, buf, sections, verify)
    finally:
        buf.close()


def read_carv2(carfile, verify=False):
    """Yield ``(digest, file, offset, size)`` for each block in a CARv2 file.

    If the file contains a supported index, it is used to locate the blocks.
    Otherwise the sections of the data payload are scanned. In both cases only
    the section headers are read. With ``verify=True`` the block data is read
    as well and checked against the CID.
    """
    buf = _mmap(carfile)
    try:
        if buf[: len(CARV2_PRAGMA)] != CARV2_PRAGMA:
            raise ValueError(f"{carfile} is not a CARv2 file")
        data_offset, data_size, index_offset = struct.unpack_from(
            "<QQQ", buf, len(CARV2_PRAGMA) + 16
        )
        first_section = _read_carv1_header(buf, data_offset, carfile)

        offsets = None
        if index_offset:
            offsets = _read_carv2_index(buf, index_offset)
        if offsets is not None:
            # identity CIDs are usually not indexed, but they are not looked
            # up in the index anyway
            sections = (
                _read_section(buf, data_offset + offset)[:3]
                for offset in sorted(set(offsets))
            )
        else:
            sections = _scan_sections(buf, first_section, data_offset + data_size)
        yield from _blocks(carfile, buf, sections, verify)
    finally:
        buf.close()


def is_carv2(carfile):
    with open(carfile, "rb") as f:
        return f.read(len(CARV2_PRAGMA)) == CARV2_PRAGMA


def read_car(carfile, verify=False):
    """Yield ``(digest, file, offset, size)`` for each block in a CARv1 or
    CARv2 file."""
    if is_carv2(carfile):
        yield from read_carv2(carfile, verify=verify)
    else:
        yield from read_carv1(carfile, verify=verify)


def save_index(df, filename):
    df.to_parquet(filename)


def generate_index(carfiles, index_fn=None, verify=False):
    """Create an index of all blocks in the given car files.

    Parameters
    ----------
    carfiles : list of str
        CARv1 and/or CARv2 files
    index_fn : str, optional
        Filename of the parquet file the index is written to
    verify : bool, default False
        Read all blocks and check them against their CIDs. This is
        considerably slower, as all data has to be read.
    """
    blocks = chain.from_iterable(read_car(fn, verify=verify) for fn in carfiles)
    index = pd.DataFrame(
        tqdm.tqdm(blocks),
        columns=["cid", "file", "offset", "size"],
    )
    index["order"] = np.arange(len(index))
    index = index.set_index("cid")
    index = index.sort_index(kind="stable")
    # The same block can be contained in several car files. All copies have
    # the same content, so only the first occurrence is kept.
    index = index[~index.index.duplicated(keep="first")]
    assert index.index.is_monotonic_increasing
    if index_fn:
        save_index(index, index_fn)
    return index
