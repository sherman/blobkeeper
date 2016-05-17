package io.blobkeeper.common.util;

import com.google.common.base.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * @author Denis Gabaydulin
 * @since 17/05/2016
 */
public class BlockElt {
    private final long id;
    private final int type;
    private final long offset;
    private final long length;
    private final long crc;

    public BlockElt(long id, int type, long offset, long length, long crc) {
        this.id = id;
        this.type = type;
        this.offset = offset;
        this.length = length;
        this.crc = crc;
    }

    public int getType() {
        return type;
    }

    public long getOffset() {
        return offset;
    }

    public long getLength() {
        return length;
    }

    public long getCrc() {
        return crc;
    }

    public long getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BlockElt that = (BlockElt) o;

        return Objects.equal(this.id, that.id) && Objects.equal(this.type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, type);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("id", id)
                .add("type", type)
                .add("offset", offset)
                .add("length", length)
                .add("crc", crc)
                .toString();
    }
}
