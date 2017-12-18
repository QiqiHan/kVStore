import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.rpc.RpcClientFactory;
import cn.helium.kvstore.rpc.RpcServer;
import javafx.scene.chart.BubbleChart;
import javafx.scene.control.Tab;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by zy812818
 * Created @ 2017/10/4.
 **/
public class MemTable {

    private int len;
//    private byte[] memTable = new byte[Config.MAX_DATA_BLOCK_SIZE];
    private ByteBuffer memTable = ByteBuffer.allocate(Config.MAX_DATA_BLOCK_SIZE);
//    private ByteBuffer memTable = ByteBuffer.allocate(Config.MAX_DATA_BLOCK_SIZE);
    private ByteBuffer index = ByteBuffer.allocate(Config.MAX_DATA_BLOCK_SIZE*2);

//    private List<PutIndex> indexes = new ArrayList<>(500);

    private volatile TableState state = TableState.free;

    private ExecutorService threadPool = Executors.newSingleThreadExecutor();

    private Future<Boolean> task;

    private int currentDataLength = 0;
    private long currentOffset = 0L;



    private int fileSize = 0; //如果到上限100M重新写一个文件
    private int tag = 0; //文件后缀

//    private volatile AtomicBoolean per_lock;
//    private byte[] endPer = new byte[]{-3};

    private byte[] prePer = new byte[]{-1};

//    private byte[] exception = new byte[]{-2, Config.KV_NUM};

    public MemTable(int len) {
        this.len = len;
        currentOffset = FileMap.offsets.getOrDefault(len,0L);
    }

    public boolean put(byte[] key, byte[] value) {
//        if (value.length + currentDataLength > Config.MAX_DATA_BLOCK_SIZE) {
//            this.persist();
//            return false;
//        } else {
            //拷贝data
//            System.arraycopy(value, 0, memTable, currentDataLength, value.length);
            memTable.put(value);
            currentDataLength += value.length;
            //设置index
            index.put((byte)(key.length+8+4));
            index.put(key);
            index.putLong(currentOffset);
            index.putInt(value.length);
//            index.put((byte)value.length);
            currentOffset+=value.length;
//            PutIndex index = new PutIndex(key, currentOffset, value.length);
//            indexes.add(index);
            return true;
//        }
    }

    public boolean isFull(byte[] value){
        if(value.length+currentDataLength >Config.MAX_DATA_BLOCK_SIZE) return true;
        return false;
    }

    public void persist() {
        //落盘前的预处理
        //保存len对应的偏移量
        this.setState(TableState.in_per);
        FileMap.offsets.put(len,currentOffset);
        //得到索引
        index.flip();
        byte[] indexContent = new byte[index.limit()];
        index.get(indexContent,0,indexContent.length);
        //得到data
        memTable.flip();
        byte[] valueContent = new byte[memTable.limit()];
        memTable.get(valueContent,0,memTable.limit());

        this.putData(len, valueContent, indexContent);

        //首先判断文件是否达到上限
        if(fileSize + memTable.limit() > Config.HDFS_FILE_SIZE){
            tag++;
            String fileIndex = Config.DIR + "/" + "index" + Config.SPLIT + len +Config.SPLIT + RpcServer.getRpcServerId()+Config.SPLIT+tag;
            String fileData = Config.DIR + "/" + "data" + Config.SPLIT + len + Config.SPLIT + RpcServer.getRpcServerId()+Config.SPLIT+tag;
            FileMap.indexFiles.put(len, fileIndex);
            FileMap.dataFiles.put(len, fileData);
            //重置fileSize
            fileSize = 0;
            currentOffset = 0;
        }

        this.setState(TableState.free);
        //clear中需要统计已经添加的fileSize
        this.clear();

        //落盘成功把相应的那部分日志删除
//        KLog klog = FileMap.KLogMap.get(Config.KV_NUM+"_"+len);
//        this.task = threadPool.submit(() ->
//        {
//            return true;
//
//        });
    }

    public TableState getState() {
        return state;
    }

    public void setState(TableState state) {
        this.state = state;
    }

    public Future<Boolean> getTask() {
        return task;
    }

//    public void setLock(Boolean state) {
//        this.per_lock.set(state);
//    }

    public void clear() {
        this.currentDataLength = 0;
        this.index.clear();
        fileSize = fileSize + memTable.limit();
        this.memTable.clear();
//        indexes.clear();
    }

//    public void remove(String key){
//        memTable.remove(key);
//    }


    private boolean putData(int len, byte[] valContents ,byte[] indexContents) {

        String fileIndex = FileMap.indexFiles.getOrDefault(len,null);
        String fileData = FileMap.dataFiles.get(len);
        if(fileIndex == null){
             fileIndex = Config.DIR + "/" + "index" + Config.SPLIT + len +Config.SPLIT + RpcServer.getRpcServerId()+Config.SPLIT+tag;
             fileData = Config.DIR + "/" + "data" + Config.SPLIT + len + Config.SPLIT + RpcServer.getRpcServerId()+Config.SPLIT+tag;
             FileMap.indexFiles.put(len,fileIndex);
             FileMap.dataFiles.put(len,fileData);
        }
        try {
            //将文件和对应的索引文件落盘
            MyTestHDFSConn.append(fileData, valContents, valContents.length);
            MyTestHDFSConn.append(fileIndex, indexContents,indexContents.length);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }
    public void setCurrentOffset(long currentOffset) {
        this.currentOffset = currentOffset;
    }
    private int[] getRestServer() {

        if (RpcServer.getRpcServerId() == 0) {
            return new int[]{1, 2};
        } else if (RpcServer.getRpcServerId() == 1) {
            return new int[]{0, 2};
        } else if (RpcServer.getRpcServerId() == 2) {
            return new int[]{0, 1};
        }

        return null;
    }


}
