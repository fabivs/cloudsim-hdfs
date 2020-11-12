package org.cloudbus.cloudsim.examples.hdfs.utils;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.hdfs.HdfsCloudlet;
import org.cloudbus.cloudsim.hdfs.HdfsDatacenter;
import org.cloudbus.cloudsim.hdfs.HdfsHost;
import org.cloudbus.cloudsim.hdfs.HdfsVm;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public final class HdfsUtils {

    // crea una lista di Vms da submittare al broker
    // NOTE: vmm is always "Xen"
    public static List<HdfsVm> createVmList(int count, int userId, int mips, int pesNumber, int ram, long bw, long size,
                                            String vmm, String cloudletSchedulerType){

        LinkedList<HdfsVm> list = new LinkedList<HdfsVm>();

        // array di VMs
        HdfsVm[] vm = new HdfsVm[count];

        // funziona così: vm è un array di dimensione "vms", nel ciclo riempiamo questo array di tante nuove vm,
        // ognuna di queste vm è anche aggiunta alla lista "list", che è ritornata alla fine, fuori dal ciclo
        for(int i = 0; i < count; i++){

            CloudletScheduler cloudletScheduler =
                    (cloudletSchedulerType.equals("Time")) ? new CloudletSchedulerTimeShared() : new CloudletSchedulerSpaceShared();

            vm[i] = new HdfsVm(i, userId, mips, pesNumber, ram, bw, size, vmm, cloudletScheduler);

            //to create a VM with a space shared scheduling policy for cloudlets:
            //vm[i] = Vm(i, userId, mips, pesNumber, ram, bw, size, priority, vmm, new CloudletSchedulerSpaceShared());

            list.add(vm[i]);
        }

        return list;
    }

    // crea una lista di Cloudlets da submittare al broker
    public static List<HdfsCloudlet> createCloudletList(int userId, int count, int destId, long length, long fileSize, long outputSize,
                                                        int pesNumber, UtilizationModel utilizationModel, List<String> blockList, int blockSize){

        LinkedList<HdfsCloudlet> list = new LinkedList<HdfsCloudlet>();

        HdfsCloudlet[] cloudlet = new HdfsCloudlet[count];

        for(int i = 0; i < count; i++){
            cloudlet[i] = new HdfsCloudlet(i, length, pesNumber, fileSize, outputSize, utilizationModel,
                    utilizationModel, utilizationModel, blockList, blockSize);
            // setting the owner of these Cloudlets
            cloudlet[i].setUserId(userId);
            cloudlet[i].setDestVmIds(destId);
            list.add(cloudlet[i]);
        }

        return list;
    }

    // crea la lista di PEs per ciascun singolo Host
    // TODO: for now it only uses PeProvisionerSimple (ma è l'unico che esiste in Cloudsim in ogni caso)
    public static List<Pe> createPeList(int num, int mips){

        List<Pe> peList = new ArrayList<Pe>();

        for (int i = 0; i < num; i++){
            peList.add(new Pe(i, new PeProvisionerSimple(mips)));
        }

        return peList;
    }

    public static List<File> createBlockList(int count, int blockSize) throws ParameterException {

        List<File> blockList = new ArrayList<File>();

        String blockName;
        File block;

        for (int i = 0; i < count; i++){

            blockName = "Block_" + String.valueOf(i);
            block = new File(blockName, blockSize);
            blockList.add(block);
        }

        return blockList;
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

    // PRINTING UTILITIES

    /**
     * Prints the Cloudlet objects
     * @param list  list of Cloudlets
     */
    public static void printCloudletList(List<Cloudlet> list) {
        int size = list.size();
        Cloudlet cloudlet;

        String indent = "    ";
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        Log.printLine("Cloudlet ID" + indent + "STATUS" + indent +
                "Data center ID" + indent + "VM ID" + indent + "Time" + indent + "Start Time" + indent + "Finish Time");

        DecimalFormat dft = new DecimalFormat("###.##");
        for (int i = 0; i < size; i++) {
            cloudlet = list.get(i);
            Log.print(indent + cloudlet.getCloudletId() + indent + indent);

            if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS){
                Log.print("SUCCESS");

                Log.printLine( indent + indent + cloudlet.getResourceId() + indent + indent + indent + cloudlet.getVmId() +
                        indent + indent + dft.format(cloudlet.getActualCPUTime()) + indent + indent + dft.format(cloudlet.getExecStartTime())+
                        indent + indent + dft.format(cloudlet.getFinishTime()));
            }
        }
    }

    /**
     * Prints the Storage objects
     * @param list  Storage list from a single Datacenter
     */
    public static void printStorageList(List<HdfsDatacenter> list){

        String indent = "    ";
        Log.printLine();
        Log.printLine("========== STORAGE STATUS ==========");

        for (HdfsDatacenter datacenter : list){
            Log.printLine("Datacenter ID: " + datacenter.getId());

            for (Storage drive : datacenter.getStorageList()){
                Log.printLine(indent + "Drive:  " + drive.getName() + ", Maximum capacity: " + drive.getCapacity() +
                        " MB, Used space: " + drive.getCurrentSize() + " MB, Free Space: " + drive.getAvailableSpace() + " MB");
                Log.printLine(indent + indent + "File list: (Number of stored files: " + drive.getNumStoredFile() + ")");

                for (String fileName : drive.getFileNameList()){
                    Log.printLine(indent + indent + indent + "File: " + fileName);
                }
            }
        }
    }
}
