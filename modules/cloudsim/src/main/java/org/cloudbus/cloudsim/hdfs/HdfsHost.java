package org.cloudbus.cloudsim.hdfs;

import org.cloudbus.cloudsim.HarddriveStorage;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.VmScheduler;
import org.cloudbus.cloudsim.provisioners.BwProvisioner;
import org.cloudbus.cloudsim.provisioners.RamProvisioner;

import java.util.List;

// IL PROBLEMA ORA È EFFETTIVAMENTE USARE QUESTO HarddriveStorage nella simulazione!!

// HdfsHost usa la class HarddriveStorage, già presente in Cloudsim, per simulare lo storage
// Un normale Host usa un semplice "long" per tenere traccia dello storage

public class HdfsHost extends Host {

    // TODO: this is, for now, completely useless
    // Il motivo: a un Host, ora come ora, non è assegnato uno o più specifici Hard Drives,
    // sono assegnati al Datacenter e ogni Host ha soltanto un numero che indica la capacità di storage
    // lascio questo field qui per ora nel caso possa servire in futuro
    private HarddriveStorage properStorage;

    /**
     * DON'T USE THIS CONSTRUCTOR FOR NOW!!
     */
    public HdfsHost(int id, RamProvisioner ramProvisioner, BwProvisioner bwProvisioner, long storage,
                    HarddriveStorage hddStorage, List<? extends Pe> peList, VmScheduler vmScheduler) {
        super(id, ramProvisioner, bwProvisioner, storage, peList, vmScheduler);
        properStorage = hddStorage;
    }

    public HdfsHost(int id, RamProvisioner ramProvisioner, BwProvisioner bwProvisioner, long storage,
                    List<? extends Pe> peList, VmScheduler vmScheduler) {
        super(id, ramProvisioner, bwProvisioner, storage, peList, vmScheduler);
    }

    public HarddriveStorage getProperStorage() {
        return properStorage;
    }

    public void setProperStorage(HarddriveStorage properStorage) {
        this.properStorage = properStorage;
    }
}
