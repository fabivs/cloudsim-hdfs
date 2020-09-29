package org.cloudbus.cloudsim;

import org.cloudbus.cloudsim.provisioners.BwProvisioner;
import org.cloudbus.cloudsim.provisioners.RamProvisioner;

import java.util.List;

// IL PROBLEMA ORA È EFFETTIVAMENTE USARE QUESTO HarddriveStorage nella simulazione!!

// HdfsHost usa la class HarddriveStorage, già presente in Cloudsim, per simulare lo storage
// Un normale Host usa un semplice "long" per tenere traccia dello storage

public class HdfsHost extends Host{

    private HarddriveStorage actualStorage;

    /**
     * Instantiates a new host.
     *
     * @param id             the host id
     * @param ramProvisioner the ram provisioner
     * @param bwProvisioner  the bw provisioner
     * @param storage        the storage capacity
     * @param hddStorage     the simulated hard drive (should make "storage" redundant)
     * @param peList         the host's PEs list
     * @param vmScheduler    the vm scheduler
     */
    public HdfsHost(int id, RamProvisioner ramProvisioner, BwProvisioner bwProvisioner, long storage,
                    HarddriveStorage hddStorage, List<? extends Pe> peList, VmScheduler vmScheduler) {
        super(id, ramProvisioner, bwProvisioner, storage, peList, vmScheduler);
        actualStorage = hddStorage;
    }

    public HarddriveStorage getActualStorage() {
        return actualStorage;
    }

    public void setActualStorage(HarddriveStorage actualStorage) {
        this.actualStorage = actualStorage;
    }
}
