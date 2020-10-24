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
                        List<String> fileList, int blockSize) {
        super(cloudletId, cloudletLength, pesNumber, cloudletFileSize, cloudletOutputSize, utilizationModelCpu, utilizationModelRam, utilizationModelBw, fileList);
        this.blockSize = blockSize;
    }

    // this method clones the given cloudlet into a new one, with the new given ID
    // this is necessary because the ID of a cloudlet is a final int

    // NOTE: non sono ancora sicuro di aver copiato tutti i fields, potrebbe mancarne qualcuno ancora
    public static HdfsCloudlet cloneCloudletAssignNewId(HdfsCloudlet cl, int newId){

        long cloudletLength = cl.getCloudletLength();
        int pesNumber = cl.getNumberOfPes();
        long cloudletFileSize = cl.getCloudletFileSize();
        long cloudletOutputSize = cl.getCloudletOutputSize();
        UtilizationModel utilizationModelCpu = cl.getUtilizationModelCpu();
        UtilizationModel utilizationModelRam = cl.getUtilizationModelRam();
        UtilizationModel utilizationModelBw = cl.getUtilizationModelBw();
        List<String> fileList = cl.getRequiredFiles();
        int blockSize = cl.getBlockSize();

        HdfsCloudlet newCl = new HdfsCloudlet(newId, cloudletLength, pesNumber, cloudletFileSize, cloudletOutputSize,
                utilizationModelCpu, utilizationModelRam, utilizationModelBw, fileList, blockSize);

        // set the user Id, because it's not part of the constructor
        int userId = cl.getUserId();
        newCl.setUserId(userId);

        return newCl;
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
