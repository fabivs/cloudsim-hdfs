package org.cloudbus.cloudsim.examples.hdfs.utils;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.hdfs.HdfsCloudlet;
import org.cloudbus.cloudsim.hdfs.HdfsHost;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public final class HdfsUtils {

    // crea una lista di Vms da submittare al broker
    // NOTE: vmm is always "Xen"
    public static List<Vm> createVmList(int userId, int count, long size, int ram, int mips, long bw, int pesNumber){

        LinkedList<Vm> list = new LinkedList<Vm>();

        String vmm = "Xen"; // VMM name

        // array di VMs
        Vm[] vm = new Vm[count];

        // funziona così: vm è un array di dimensione "vms", nel ciclo riempiamo questo array di tante nuove vm,
        // ognuna di queste vm è anche aggiunta alla lista "list", che è ritornata alla fine, fuori dal ciclo
        for(int i = 0; i < count; i++){
            vm[i] = new Vm(i, userId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());

            //to create a VM with a space shared scheduling policy for cloudlets:
            //vm[i] = Vm(i, userId, mips, pesNumber, ram, bw, size, priority, vmm, new CloudletSchedulerSpaceShared());

            list.add(vm[i]);
        }

        return list;
    }

    // crea una lista di Cloudlets da submittare al broker
    public static List<HdfsCloudlet> createCloudletList(int userId, int count, long length, long fileSize, long outputSize,
                                                        int pesNumber, UtilizationModel utilizationModel, List<String> blockList, int blockSize){

        LinkedList<HdfsCloudlet> list = new LinkedList<HdfsCloudlet>();

        HdfsCloudlet[] cloudlet = new HdfsCloudlet[count];

        for(int i = 0; i < count; i++){
            cloudlet[i] = new HdfsCloudlet(i, length, pesNumber, fileSize, outputSize, utilizationModel,
                    utilizationModel, utilizationModel, blockList, blockSize);
            // setting the owner of these Cloudlets
            cloudlet[i].setUserId(userId);
            list.add(cloudlet[i]);
        }

        return list;
    }

    // crea la lista di PEs per ciascun singolo Host
    // TODO: for now it only uses PeProvisionerSimple
    public static List<Pe> createPeList(int num, int mips){

        List<Pe> peList = new ArrayList<Pe>();

        for (int i = 0; i < num; i++){
            peList.add(new Pe(i, new PeProvisionerSimple(mips)));
        }

        return peList;
    }

    // questa linked list sarà poi la linked list di storage in DatacenterCharacteristics
    public static LinkedList<Storage> createStorageList(int num, int storageSize) throws ParameterException {

        LinkedList<Storage> storageList = new LinkedList<Storage>();

        for (int i = 0; i < num; i++){
            String name = "HDD_" + String.valueOf(i);
            storageList.add(new HarddriveStorage(name, storageSize));
        }

        return storageList;
    }

    // crea la lista di Hosts in un Datacenter
    // TODO: Per ora il Vm scheduler è solo Time Shared e gli altri provisioners sono solo le versioni "Simple"
    public static List<HdfsHost> createHostList(int num, int ram, int storageSize, int bw, int pesNum, int mips){

        // if it works as intended, ogni singolo host deve crearsi la propria istanza di una PeList

        List<HdfsHost> hostList = new ArrayList<HdfsHost>();

        for (int i = 0; i < num; i++){

            List<Pe> peList = createPeList(pesNum, mips);

            hostList.add(new HdfsHost(
                    i,
                    new RamProvisionerSimple(ram),
                    new BwProvisionerSimple(bw),
                    storageSize,
                    peList,
                    new VmSchedulerTimeShared(peList)));
        }

        return hostList;
    }
}
