package com.my.sqlcache;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class SQLCacheConnector implements AutoCloseable {

    private Socket client = null;
    private ByteArrayOutputStream sqlOut;

    private final int BUFFER_LENGTH = 1024000;
    private final int MAX_WAIT_TIME = 60000;  // 毫秒

    private byte[] buffer = new byte[BUFFER_LENGTH];

    private boolean busy = false;
    private boolean inTransaction = false;

    private SQLCacheErrorCode errorCode = SQLCacheErrorCode.scecNone;

    public SQLCacheConnector() {

    }

    public void close() throws Exception {
        disconnect();
    }

    public void connect(String serverIp, int port) throws IOException {
        client = new Socket(serverIp, port);
        client.setTcpNoDelay(true);
        sqlOut = new ByteArrayOutputStream();
    }

    public void disconnect() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    public int update(String sql, Object... params) throws Exception {
        assertConnect();

        if (sql.charAt(sql.length() - 1) != ';') {
            sql = sql + ";";
        }


        try (DataOutputStream dOut = new DataOutputStream(sqlOut)) {
            byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_8);
            dOut.writeInt(sqlBytes.length);
            dOut.write(sqlBytes);
            dOut.writeShort(params.length / 2);
            for (int i = 0; i < params.length; i += 2) {
                writeParam((ParamDataType) params[i], params[i + 1], dOut);
            }
        }

        if (inTransaction) {
            return 0;
        }

        client.getOutputStream().write(sqlOut.toByteArray());
        sqlOut.reset();

        byte[] data = waitResponse();
        if (data == null) {
            errorCode = SQLCacheErrorCode.scecServerError;
            return 0;
        }

        SQLResultInputStream in = new SQLResultInputStream(data);
        errorCode = SQLCacheErrorCode.valueOf(in.readUByte());
        return in.readInt();
    }

    public SQLResultSet select(String sql, Object... params) throws Exception {
        assertConnect();

        if (sql.charAt(sql.length() - 1) != ';') {
            sql = sql + ";";
        }

        try (DataOutputStream dOut = new DataOutputStream(sqlOut)) {
            byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_8);
            dOut.writeInt(sqlBytes.length);
            dOut.write(sqlBytes);
            dOut.writeShort(params.length / 2);
            for (int i = 0; i < params.length; i += 2) {
                writeParam((ParamDataType) params[i], params[i + 1], dOut);
            }
        }

        client.getOutputStream().write(sqlOut.toByteArray());
        sqlOut.reset();
        byte[] data = waitResponse();
        if (data == null) {
            errorCode = SQLCacheErrorCode.scecServerError;
            return null;
        }

        SQLResultInputStream in = new SQLResultInputStream(data);
        errorCode = SQLCacheErrorCode.valueOf(in.readUByte());
        if (errorCode != SQLCacheErrorCode.scecNone) {
            return null;
        }

        return new SQLResultSet(in);
    }

    public void begin() throws Exception {
        try (DataOutputStream dOut = new DataOutputStream(sqlOut)) {
            byte[] bytes = "BEGIN".getBytes(StandardCharsets.UTF_8);
            dOut.writeInt(bytes.length);
            dOut.write(bytes);
        }
        inTransaction = true;
    }

    public int commit() throws Exception {
        try (DataOutputStream dOut = new DataOutputStream(sqlOut)) {
            byte[] bytes = "COMMIT".getBytes(StandardCharsets.UTF_8);
            dOut.writeInt(bytes.length);
            dOut.write(bytes);
        }
        inTransaction = false;
        client.getOutputStream().write(sqlOut.toByteArray());
        sqlOut.reset();

        byte[] data = waitResponse();
        if (data == null) {
            errorCode = SQLCacheErrorCode.scecServerError;
            return 0;
        }

        SQLResultInputStream in = new SQLResultInputStream(data);
        errorCode = SQLCacheErrorCode.valueOf(in.readUByte());
        return in.readInt();
    }

    /**
     * 输出监测信息
     * @return
     * @throws Exception
     */
    public String monitor() throws Exception {
        assertConnect();

        try (DataOutputStream dOut = new DataOutputStream(sqlOut)) {
            byte[] bytes = "MONITOR".getBytes(StandardCharsets.UTF_8);
            dOut.writeInt(bytes.length);
            dOut.write(bytes);
        }

        client.getOutputStream().write(sqlOut.toByteArray());
        sqlOut.reset();
        byte[] data = waitResponse();
        if (data == null) {
            return "";
        }

        return new String(data);
    }

    private void assertConnect() throws Exception {
        if (client == null) {
            throw new Exception("Isn't Connect To Server");
        }
    }

    private byte[] waitResponse() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int timeCount = 0;
        InputStream in = client.getInputStream();
        while(timeCount < MAX_WAIT_TIME / 10 && in.available() <= 0) {
            Thread.sleep(10);
            ++timeCount;
        }

        if (in.available() <= 0) {
            return null;
        }

        while(true) {
            int readLen = in.read(buffer);
            if (readLen > 0) {
                out.write(buffer, 0, readLen);
            }

            if (readLen < BUFFER_LENGTH) {
                break;
            }
        }

        return out.toByteArray();
    }

    public boolean isBusy() {
        return busy;
    }

    public void setBusy(boolean busy) {
        this.busy = busy;
    }

    private void writeParam(ParamDataType dataType, Object param, DataOutputStream out) throws IOException {
        out.write(dataType.getValue());
        if (param == null) {
            out.write(1);
            return;
        }
        out.write(0);

        switch (dataType) {
            case pdtString: {
                byte[] data = ((String) param).getBytes(StandardCharsets.UTF_8);
                out.writeInt(data.length);
                out.write(data);
                break;
            }
            case pdtBool: {
                out.write(param.equals(true) ? 1 : 0);
                break;
            }
            case pdtInt: {
                if (param instanceof Integer) {
                    out.writeInt((Integer) param);
                }
                else if (param instanceof Long) {
                    out.writeInt(((Long) param).intValue());
                }
                else if (param instanceof Double) {
                    out.writeInt(((Double) param).intValue());
                }
                else {
                    throw new RuntimeException("param is not Integer compatible");
                }

                break;
            }
            case pdtLong: {
                if (param instanceof Long) {
                    out.writeLong((Long)param);
                }
                else if (param instanceof Integer) {
                    out.writeLong((Integer) param);
                }
                else if (param instanceof Double) {
                    out.writeLong(((Double) param).longValue());
                }
                else {
                    throw new RuntimeException("param is not Long compatible");
                }
                break;
            }
            case pdtDouble: {
                if (param instanceof Double) {
                    out.writeDouble((Double)param);
                }
                else if (param instanceof Integer) {
                    out.writeDouble(((Integer) param).doubleValue());
                }
                else if (param instanceof Long) {
                    out.writeDouble(((Long) param).doubleValue());
                }
                else {
                    throw new RuntimeException("param is not Double compatible");
                }
                break;
            }
            case pdtBlob: {
                out.writeInt(((byte[]) param).length);
                out.write((byte[]) param);
                break;
            }
            default:
                break;
        }
    }
}
