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

    public static List<Vm> createVmList(){

        // TODO

        return null;
    }

    public static List<HdfsCloudlet> createCloudletList(){

        // TODO

        return null;
    }

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
