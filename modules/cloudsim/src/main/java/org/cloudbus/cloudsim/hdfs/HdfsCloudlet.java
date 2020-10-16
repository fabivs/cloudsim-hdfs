package org.cloudbus.cloudsim.hdfs;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.UtilizationModel;

import java.util.List;

public class HdfsCloudlet extends Cloudlet {

    // the id of the original vm, so we don't lose it
    protected int sourceVmId;

    // the id of the Data Node VM where the data block will be written
    protected int destVmId;

    // we need this to have the information necessary to simulate the write in the DN, we can't simply search it
    // by name like in the Client, because the file is not already present in the Database
    protected int blockSize;



    /**
     * Non so se i costruttori vanno reimplementati tutti, quindi per ora ho messo solo quello che mi interessa, esteso
     * come serve a me (ho aggiunto hdfsBlock, che contiene le info per la scrittura del file nel DN)
     */

    public HdfsCloudlet(int cloudletId, long cloudletLength, int pesNumber, long cloudletFileSize, long cloudletOutputSize,
                        UtilizationModel utilizationModelCpu, UtilizationModel utilizationModelRam, UtilizationModel utilizationModelBw,
                        List<String> fileList) {
        super(cloudletId, cloudletLength, pesNumber, cloudletFileSize, cloudletOutputSize, utilizationModelCpu, utilizationModelRam, utilizationModelBw, fileList);
    }

    // Getters and Setters

    public int getSourceVmId() {
        return sourceVmId;
    }

    public void setSourceVmId(int sourceVmId) {
        this.sourceVmId = sourceVmId;
    }

    public int getDestVmId() {
        return destVmId;
    }

    public void setDestVmId(final int destVmId) {
        this.destVmId = destVmId;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }
}
