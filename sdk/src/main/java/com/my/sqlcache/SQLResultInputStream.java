package com.my.sqlcache;

public class SQLResultInputStream {

    private byte[] data = null;
    private int offset = 0;
    private int len = 0;

    public SQLResultInputStream(byte[] data) {
        this.data = data;
        this.len = data.length;
        this.offset = 0;
    }

    public SQLResultInputStream(byte[] data, int len) {
        this.data = data;
        this.len = len;
        this.offset = 0;
    }

    public byte[] data() {
        return data;
    }

    public int byteLength() {
        return len;
    }

    public byte readByte() {
        int ch = data[offset++] & 255;
        return (byte)ch;
    }

    public short readUByte() {
        int ch = data[offset++] & 255;
        return (short)ch;
    }

    public boolean readBoolean() {
        return readByte() == 1;
    }

    public short readShort() {
        int ch1 = data[offset++] & 255;
        int ch2 = data[offset++] & 255;
        return (short)((ch1 << 8) + (ch2 << 0));
    }

    public int readUShort() {
        int ch1 = data[offset++] & 255;
        int ch2 = data[offset++] & 255;
        return (ch1 << 8) + (ch2 << 0);
    }

    public int readInt() {
        int ch1 = data[offset++] & 255;
        int ch2 = data[offset++] & 255;
        int ch3 = data[offset++] & 255;
        int ch4 = data[offset++] & 255;
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }

    public long readUInt() {
        int ch1 = data[offset] & 255;
        int ch2 = data[offset++] & 255;
        int ch3 = data[offset++] & 255;
        int ch4 = data[offset++] & 255;
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }

    public long readLong() {
        return ((long)(data[offset++]) << 56) +
                ((long)(data[offset++] & 255) << 48) +
                ((long)(data[offset++] & 255) << 40) +
                ((long)(data[offset++] & 255) << 32) +
                ((long)(data[offset++] & 255) << 24) +
                ((data[offset++] & 255) << 16) +
                ((data[offset++] & 255) << 8) +
                ((data[offset++] & 255) << 0);
    }

    public float readFloat() {
        return Float.intBitsToFloat(readInt());
    }

    public double readDouble() {
        return Double.longBitsToDouble(readLong());
    }

    public String readString() {
        int length = readUShort();
        if (length == 0) {
            return "";
        }

        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < length; ++i) {
            stringBuilder.append((char)readByte());
        }
        return stringBuilder.toString();
    }

    public String readText() {
        int length = readUShort();
        if (length == 0) {
            return "";
        }

        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < length; ++i) {
            stringBuilder.append((char)readByte());
        }
        return stringBuilder.toString();
    }

    public byte[] readBlock() {
        int length = readInt();
        byte[] result = new byte[length];
        for (int i = 0; i < length; ++i) {
            result[i] = readByte();
        }
        return result;
    }

    public void skip(int size) {
        offset += size;
    }
}
