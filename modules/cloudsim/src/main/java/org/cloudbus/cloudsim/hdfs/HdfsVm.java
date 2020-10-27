package org.cloudbus.cloudsim.hdfs;

import org.cloudbus.cloudsim.CloudletScheduler;
import org.cloudbus.cloudsim.Vm;

public class HdfsVm extends Vm {

    protected int hdfsType;

    /**
     * Creates a new Vm object.
     *
     * @param id                unique ID of the VM
     * @param userId            ID of the VM's owner
     * @param mips              the mips
     * @param numberOfPes       amount of CPUs
     * @param ram               amount of ram
     * @param bw                amount of bandwidth
     * @param size              The size the VM image size (the amount of storage it will use, at least initially).
     * @param vmm               virtual machine monitor
     * @param cloudletScheduler cloudletScheduler policy for cloudlets scheduling
     * @pre id >= 0
     * @pre userId >= 0
     * @pre size > 0
     * @pre ram > 0
     * @pre bw > 0
     * @pre cpus > 0
     * @pre priority >= 0
     * @pre cloudletScheduler != null
     * @post $none
     */
    public HdfsVm(int id, int userId, double mips, int numberOfPes, int ram, long bw, long size, String vmm, CloudletScheduler cloudletScheduler) {
        super(id, userId, mips, numberOfPes, ram, bw, size, vmm, cloudletScheduler);
    }

    public int getHdfsType() {
        return hdfsType;
    }

    public void setHdfsType(int hdfsType) {
        this.hdfsType = hdfsType;
    }
}
