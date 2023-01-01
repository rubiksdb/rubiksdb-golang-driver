package network

import (
    "errors"
    "wkk/common/log"
    "wkk/common/misc"
    "wkk/common/serd"
)

const EOMMagic = 0x69e1

func Serialize(dst []byte, magic uint16, nbufs []*Nbuf, blobs [][]byte) []byte {
    mark := dst
    dst = serd.Put64LE(2, dst, uint64(magic))

    pLen := dst[:3]
    dst = serd.Put64LE(3, dst, 0)

    dst = serd.Put64LE(1, dst, uint64(len(nbufs)))
    pCnt := dst[:1]
    dst = serd.Put64LE(1, dst, 255)

    for _, nb := range nbufs {
        dst = nb.Serialize(dst)
    }

    countNonEmptyBlobs := uint64(0)
    for i, b := range blobs {
        if len(b) > 0 {
            dst = serd.Put64LE(1, dst, uint64(i))
            dst = serd.Put64LE(3, dst, uint64(len(b)))

            misc.Assert(len(dst) > len(b))
            copy(dst, b)
            dst = dst[len(b):]
            countNonEmptyBlobs += 1
        }
    }

    dst = serd.Put64LE(2, dst, EOMMagic)

    mlen := len(mark) - len(dst)
    serd.Put64LE(3, pLen, uint64(mlen))
    serd.Put64LE(1, pCnt, countNonEmptyBlobs)
    return mark[:mlen]
}

func Deserialize(src []byte, nbufs []*Nbuf, blobs [][]byte) error {
    var err error
    var mlen, countN, countB, bi, blen, magic uint64

    src = src[2:]   // skip magic which has been checked by Consumable

    mlen, src, err = serd.Get64LE(3, src)
    if err != nil {
        return err
    }
    if int(mlen) != len(src) + 2 + 3 {
        return errors.New("bad message")
    }

    countN, src, err = serd.Get64LE(1, src)
    if err != nil {
        return err
    }
    if int(countN) != len(nbufs) {
        return errors.New("bad message")
    }

    countB, src, err = serd.Get64LE(1, src)
    if int(countB) > len(blobs) {
        return errors.New("bad message")
    }

    for i := 0; i < int(countN); i += 1 {
        src, err = nbufs[i].Deserialize(src)
        if err != nil {
            return err
        }
    }

    for i := 0; i < int(countB); i += 1 {
        bi, src, err = serd.Get64LE(1, src)
        if err != nil {
            return err
        }

        blen, src, err = serd.Get64LE(3, src)
        if err != nil {
            return err
        } else if len(src) < int(blen) {
            return errors.New("bad message")
        }
        blobs[bi], src = src[:blen], src[blen:]
    }

    if magic, src, err = serd.Get64LE(2, src); err != nil {
        return err
    } else if magic != EOMMagic {
        return errors.New("bad magic")
    }
    return nil
}

func Consumable(src []byte, magic uint16) (int, uint64, uint64) {
    var mmagic, mlen uint64
    var nb Nbuf
    var err error

    if len(src) < 2 + 3 {
        return 0, 0, 0  // not enough bytes, not error
    }

    mmagic, src, err = serd.Get64LE(2, src)
    if err != nil {
        return -1, 0, 0
    }

    if uint16(mmagic) != magic {
        log.Info("bad magic!")
        return -1, 0, 0
    }

    mlen, src, _ = serd.Get64LE(3, src)
    if int(mlen) > 2 + 3 + len(src) {
        return 0, 0, 0   // not enough bytes, not error
    }

    _, err = nb.Deserialize(src[2:])    // [2:] to skip nbuf/blob count
    if err != nil || !nb.Have(Bit(TagRequestID) |
                              Bit(TagClientID)) {
        log.Info("bad request!")
        return -1, 0, 0
    }

    return int(mlen), nb.Get(TagRequestID), nb.Get(TagClientID)
}
