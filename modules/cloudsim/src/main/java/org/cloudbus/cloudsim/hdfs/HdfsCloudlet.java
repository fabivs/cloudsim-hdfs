package org.cloudbus.cloudsim.hdfs;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;

import java.util.List;

import static org.cloudbus.cloudsim.core.CloudSimTags.HDFS_DN;

public class HdfsCloudlet extends Cloudlet {

    // the HDFS type: either Client or Data Node, by default it's going to be Data Node
    protected int hdfsType;

    // the id of the original vm, so we don't lose it
    protected int sourceVmId;

    // the id of the Data Node VM where the data block will be written
    protected List<Integer> destVmIds;

    // the required file (Cloudlet has a List of required files, but for my model, I just need a single file)
    protected File requiredFile;    // TODO: sistemare questa storia eventualmente

    // we need this to have the information necessary to simulate the write in the DN, we can't simply search it
    // by name like in the Client, because the file is not already present in the Database
    protected int blockSize;

    // number of replicas desired for the file of this cloudlet
    protected int replicaNum;

    /**
     * Non so se i costruttori vanno re-implementati tutti, quindi per ora ho messo solo quello che mi interessa, esteso
     * come serve a me (ho aggiunto hdfsBlock, che contiene le info per la scrittura del file nel DN)
     */

    public HdfsCloudlet(int cloudletId, long cloudletLength, int pesNumber, long cloudletFileSize, long cloudletOutputSize,
                        UtilizationModel utilizationModelCpu, UtilizationModel utilizationModelRam, UtilizationModel utilizationModelBw,
                        List<String> fileList, int blockSize, int replicaNum) {
        super(cloudletId, cloudletLength, pesNumber, cloudletFileSize, cloudletOutputSize, utilizationModelCpu, utilizationModelRam, utilizationModelBw, fileList);
        this.blockSize = blockSize;
        this.replicaNum = replicaNum;
        // by default the type will be Data Node, this is because I made this change after writing all the file transfer code
        this.hdfsType = HDFS_DN;
    }

    // costruttore nel caso non è specificato il numero di repliche (il valore è settato a 0)
    public HdfsCloudlet(int cloudletId, long cloudletLength, int pesNumber, long cloudletFileSize, long cloudletOutputSize,
                        UtilizationModel utilizationModelCpu, UtilizationModel utilizationModelRam, UtilizationModel utilizationModelBw,
                        List<String> fileList, int blockSize) {
        super(cloudletId, cloudletLength, pesNumber, cloudletFileSize, cloudletOutputSize, utilizationModelCpu, utilizationModelRam, utilizationModelBw, fileList);
        this.blockSize = blockSize;
        this.replicaNum = 0;
        // by default the type will be Data Node, this is because I made this change after writing all the file transfer code
        this.hdfsType = HDFS_DN;
    }

    // this method clones the given cloudlet into a new one, with the new given ID
    // this is necessary because the ID of a cloudlet is a final int

    // NOTE: non sono ancora sicuro di aver copiato tutti i fields, potrebbe mancarne qualcuno ancora
    public static HdfsCloudlet cloneCloudletAssignNewId(HdfsCloudlet cl, int newId){

        long cloudletLength = cl.getCloudletLength();
        int pesNumber = cl.getNumberOfPes();
        long cloudletFileSize = cl.getCloudletFileSize();
        long cloudletOutputSize = cl.getCloudletOutputSize();
        // TODO: per ora re-instanzio tutto come utilization model full, dovrei controllare che utilization model usa il cloudlet originale, ma uso solo full for now
        UtilizationModel utilizationModelCpu = new UtilizationModelFull();
        UtilizationModel utilizationModelRam = new UtilizationModelFull();
        UtilizationModel utilizationModelBw = new UtilizationModelFull();
        List<String> fileList = cl.getRequiredFiles();
        int blockSize = cl.getBlockSize();

        HdfsCloudlet newCl = new HdfsCloudlet(newId, cloudletLength, pesNumber, cloudletFileSize, cloudletOutputSize,
                utilizationModelCpu, utilizationModelRam, utilizationModelBw, fileList, blockSize);

        // set the user Id, because it's not part of the constructor
        int userId = cl.getUserId();
        newCl.setUserId(userId);

        // set the destination vms ids
        newCl.setDestVmIds(cl.getDestVmIds());

        return newCl;
    }

    // Getters and Setters

    public int getHdfsType() {
        return hdfsType;
    }

    public void setHdfsType(int hdfsType) {
        this.hdfsType = hdfsType;
    }

    public int getSourceVmId() {
        return sourceVmId;
    }

    public void setSourceVmId(int sourceVmId) {
        this.sourceVmId = sourceVmId;
    }

    public List<Integer> getDestVmIds() {
        return destVmIds;
    }

    public void setDestVmIds(final List<Integer> destVmIds) {
        this.destVmIds = destVmIds;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    public int getReplicaNum() {
        return replicaNum;
    }

    public void setReplicaNum(int replicaNum) {
        this.replicaNum = replicaNum;
    }
}
