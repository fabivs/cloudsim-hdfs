package org.cloudbus.cloudsim.hdfs;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;

import java.util.Iterator;
import java.util.List;

public class HdfsDatacenter extends Datacenter {


    /**
     * Allocates a new Datacenter object.
     *
     * @param name               the name to be associated with this entity (as required by the super class)
     * @param characteristics    the characteristics of the datacenter to be created
     * @param vmAllocationPolicy the policy to be used to allocate VMs into hosts
     * @param storageList        a List of storage elements, for data simulation
     * @param schedulingInterval the scheduling delay to process each datacenter received event
     * @throws Exception when one of the following scenarios occur:
     *                   <ul>
     *                     <li>creating this entity before initializing CloudSim package
     *                     <li>this entity name is <tt>null</tt> or empty
     *                     <li>this entity has <tt>zero</tt> number of PEs (Processing Elements). <br/>
     *                     No PEs mean the Cloudlets can't be processed. A CloudResource must contain
     *                     one or more Machines. A Machine must contain one or more PEs.
     *                   </ul>
     * @pre name != null
     * @pre resource != null
     * @post $none
     */
    public HdfsDatacenter(String name, DatacenterCharacteristics characteristics, VmAllocationPolicy vmAllocationPolicy,
                          List<Storage> storageList, double schedulingInterval) throws Exception {
        super(name, characteristics, vmAllocationPolicy, storageList, schedulingInterval);
    }

    @Override
    public void processEvent(SimEvent ev) {
        int srcId = -1;

        switch (ev.getTag()) {
            // Resource characteristics inquiry
            case CloudSimTags.RESOURCE_CHARACTERISTICS:
                srcId = ((Integer) ev.getData()).intValue();
                sendNow(srcId, ev.getTag(), getCharacteristics());
                break;

            // Resource dynamic info inquiry
            case CloudSimTags.RESOURCE_DYNAMICS:
                srcId = ((Integer) ev.getData()).intValue();
                sendNow(srcId, ev.getTag(), 0);
                break;

            case CloudSimTags.RESOURCE_NUM_PE:
                srcId = ((Integer) ev.getData()).intValue();
                int numPE = getCharacteristics().getNumberOfPes();
                sendNow(srcId, ev.getTag(), numPE);
                break;

            case CloudSimTags.RESOURCE_NUM_FREE_PE:
                srcId = ((Integer) ev.getData()).intValue();
                int freePesNumber = getCharacteristics().getNumberOfFreePes();
                sendNow(srcId, ev.getTag(), freePesNumber);
                break;

            // New Cloudlet arrives
            case CloudSimTags.CLOUDLET_SUBMIT:
                processCloudletSubmit(ev, false);
                break;

            // New Cloudlet arrives, but the sender asks for an ack
            case CloudSimTags.CLOUDLET_SUBMIT_ACK:
                processCloudletSubmit(ev, true);
                break;

            // Cancels a previously submitted Cloudlet
            case CloudSimTags.CLOUDLET_CANCEL:
                processCloudlet(ev, CloudSimTags.CLOUDLET_CANCEL);
                break;

            // Pauses a previously submitted Cloudlet
            case CloudSimTags.CLOUDLET_PAUSE:
                processCloudlet(ev, CloudSimTags.CLOUDLET_PAUSE);
                break;

            // Pauses a previously submitted Cloudlet, but the sender
            // asks for an acknowledgement
            case CloudSimTags.CLOUDLET_PAUSE_ACK:
                processCloudlet(ev, CloudSimTags.CLOUDLET_PAUSE_ACK);
                break;

            // Resumes a previously submitted Cloudlet
            case CloudSimTags.CLOUDLET_RESUME:
                processCloudlet(ev, CloudSimTags.CLOUDLET_RESUME);
                break;

            // Resumes a previously submitted Cloudlet, but the sender
            // asks for an acknowledgement
            case CloudSimTags.CLOUDLET_RESUME_ACK:
                processCloudlet(ev, CloudSimTags.CLOUDLET_RESUME_ACK);
                break;

            // Moves a previously submitted Cloudlet to a different resource
            case CloudSimTags.CLOUDLET_MOVE:
                processCloudletMove((int[]) ev.getData(), CloudSimTags.CLOUDLET_MOVE);
                break;

            // Moves a previously submitted Cloudlet to a different resource
            case CloudSimTags.CLOUDLET_MOVE_ACK:
                processCloudletMove((int[]) ev.getData(), CloudSimTags.CLOUDLET_MOVE_ACK);
                break;

            // Checks the status of a Cloudlet
            case CloudSimTags.CLOUDLET_STATUS:
                processCloudletStatus(ev);
                break;

            // Ping packet
            case CloudSimTags.INFOPKT_SUBMIT:
                processPingRequest(ev);
                break;

            case CloudSimTags.VM_CREATE:
                processVmCreate(ev, false);
                break;

            case CloudSimTags.VM_CREATE_ACK:
                processVmCreate(ev, true);
                break;

            case CloudSimTags.VM_DESTROY:
                processVmDestroy(ev, false);
                break;

            case CloudSimTags.VM_DESTROY_ACK:
                processVmDestroy(ev, true);
                break;

            case CloudSimTags.VM_MIGRATE:
                processVmMigrate(ev, false);
                break;

            case CloudSimTags.VM_MIGRATE_ACK:
                processVmMigrate(ev, true);
                break;

            case CloudSimTags.VM_DATA_ADD:
                processDataAdd(ev, false);
                break;

            case CloudSimTags.VM_DATA_ADD_ACK:
                processDataAdd(ev, true);
                break;

            case CloudSimTags.VM_DATA_DEL:
                processDataDelete(ev, false);
                break;

            case CloudSimTags.VM_DATA_DEL_ACK:
                processDataDelete(ev, true);
                break;

            case CloudSimTags.VM_DATACENTER_EVENT:
                updateCloudletProcessing();
                checkCloudletCompletion();
                break;

            /**
             *  HDFS TAGS
             */

            // Submit del file transfer cloudlet (Data cloudlet)
            case CloudSimTags.HDFS_CLIENT_CLOUDLET_SUBMIT:
                processClientCloudletSubmit(ev, false);
                break;

            // Ack del file transfer cloudlet (Data cloudlet)
            case CloudSimTags.HDFS_CLIENT_CLOUDLET_SUBMIT_ACK:
                processClientCloudletSubmit(ev, true);
                break;

            // Submit del file transfer cloudlet (Data cloudlet)
            case CloudSimTags.HDFS_DN_CLOUDLET_SUBMIT:
                processDNCloudletSubmit(ev, false);
                break;

            // Ack del file transfer cloudlet (Data cloudlet)
            case CloudSimTags.HDFS_DN_CLOUDLET_SUBMIT_ACK:
                processDNCloudletSubmit(ev, true);
                break;

            // other unknown tags are processed by this method
            default:
                processOtherEvent(ev);
                break;
        }
    }

    /**
     * Processes a Client Cloudlet submission, which reads a block from disk and sends it to the DN VM over the network
     *
     * @param ev information about the event just happened
     * @param ack indicates if the event's sender expects to receive
     * an acknowledge message when the event finishes to be processed
     *
     * @pre ev != null
     * @post $none
     */

    // ho aggiunto i due parametri di processCloudletMove
    protected void processClientCloudletSubmit(SimEvent ev, boolean ack) {

        // update nel datacenter di tutti i cloudlets in tutti gli hosts e setta il delay nel datacenter stesso
        // per quando è possibile iniziare la prossima operazione
        updateCloudletProcessing();

        try {
            // gets the Cloudlet object
            HdfsCloudlet cl = (HdfsCloudlet) ev.getData();

            // checks whether this Cloudlet has finished or not
            // TODO: controllare questo blocco
            if (cl.isFinished()) {
                String name = CloudSim.getEntityName(cl.getUserId());
                Log.printConcatLine(getName(), ": Warning - Cloudlet #", cl.getCloudletId(), " owned by ", name,
                        " is already completed/finished.");
                Log.printLine("Therefore, it is not being executed again");
                Log.printLine();

                // NOTE: If a Cloudlet has finished, then it won't be processed.
                // So, if ack is required, this method sends back a result.
                // If ack is not required, this method don't send back a result.
                // Hence, this might cause CloudSim to be hanged since waiting
                // for this Cloudlet back.
                if (ack) {
                    int[] data = new int[3];
                    data[0] = getId();
                    data[1] = cl.getCloudletId();
                    data[2] = CloudSimTags.FALSE;

                    // unique tag = operation tag
                    int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
                    sendNow(cl.getUserId(), tag, data);
                }

                // Cambiamento del tag: in modo che il broker sa che è tornato il cloudlet che ha letto il file,
                // ora può inviare il cloudlet che scriverà il file alla vm del Data Node
                sendNow(cl.getUserId(), CloudSimTags.HDFS_CLIENT_CLOUDLET_RETURN, cl);

                return;
            }

            // settiamo nel cloudlet le risorse di questo specifico Datacenter in cui ci troviamo
            cl.setResourceParameter(
                    getId(), getCharacteristics().getCostPerSecond(),
                    getCharacteristics().getCostPerBw());

            int userId = cl.getUserId();
            int vmId = cl.getVmId();

            // il tempo necessario per leggere i requiredFiles dal disco
            double fileTransferTime = predictFileTransferTime(cl.getRequiredFiles());

            // troviamo l'host in cui si trova la vm del cloudlet
            Host host = getVmAllocationPolicy().getHost(vmId, userId);
            // get the vm as well
            Vm vm = host.getVm(vmId, userId);
            CloudletScheduler scheduler = vm.getCloudletScheduler();
            // submittiamo il cloudlet, e il metodo ci ritorna il finish time
            double estimatedFinishTime = scheduler.cloudletSubmit(cl, fileTransferTime);

            // if this cloudlet is in the exec queue
            if (estimatedFinishTime > 0.0 && !Double.isInfinite(estimatedFinishTime)) {
                estimatedFinishTime += fileTransferTime;

                // il Datacenter invia a se stesso l'evento generico che lo fa attendere il tempo necessario
                send(getId(), estimatedFinishTime, CloudSimTags.VM_DATACENTER_EVENT);
            }

            if (ack) {
                int[] data = new int[3];
                data[0] = getId();
                data[1] = cl.getCloudletId();
                data[2] = CloudSimTags.TRUE;

                // unique tag = operation tag
                int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
                sendNow(cl.getUserId(), tag, data);
            }

            // ...
            // send();

        } catch (ClassCastException c) {
            Log.printLine(getName() + ".processCloudletSubmit(): " + "ClassCastException error.");
            c.printStackTrace();
        } catch (Exception e) {
            Log.printLine(getName() + ".processCloudletSubmit(): " + "Exception error.");
            e.printStackTrace();
        }


        // questo metodo è quello che invia i Cloudlet return
        checkCloudletCompletion();
    }

    // il metodo per il Data Node che deve scrivere il file su disco
    // per scrivere il file semplicemente creiamo un nuovo file object delle dimensioni giuste e lo "scriviamo" su disco

    // il metodo predictFileTransferTime() viene sostituito con un metodo che scrive il file su disco e ritorna il tempo
    // stimato per effettuare l'operazione
    protected void processDNCloudletSubmit(SimEvent ev, boolean ack) {

        // update nel datacenter di tutti i cloudlets in tutti gli hosts e setta il delay nel datacenter stesso
        // per quando è possibile iniziare la prossima operazione
        updateCloudletProcessing();

        try {
            // gets the Cloudlet object
            HdfsCloudlet cl = (HdfsCloudlet) ev.getData();

            // checks whether this Cloudlet has finished or not
            if (cl.isFinished()) {
                String name = CloudSim.getEntityName(cl.getUserId());
                Log.printConcatLine(getName(), ": Warning - Cloudlet #", cl.getCloudletId(), " owned by ", name,
                        " is already completed/finished.");
                Log.printLine("Therefore, it is not being executed again");
                Log.printLine();

                // NOTE: If a Cloudlet has finished, then it won't be processed.
                // So, if ack is required, this method sends back a result.
                // If ack is not required, this method don't send back a result.
                // Hence, this might cause CloudSim to be hanged since waiting
                // for this Cloudlet back.
                if (ack) {
                    int[] data = new int[3];
                    data[0] = getId();
                    data[1] = cl.getCloudletId();
                    data[2] = CloudSimTags.FALSE;

                    // unique tag = operation tag
                    int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
                    sendNow(cl.getUserId(), tag, data);
                }

                // prima di rimandare il cloudlet indietro, dobbiamo effettuare il file transfer
                // (però magari a questo punto di "cloudlet finished" voglio intendere che è finito
                // anche il file transfer) ?????????

                // ... TODO: tutto questo blocco di codice va visto, non ci capisco un cazzo

                sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, cl);

                return;
            }

            // settiamo nel cloudlet le risorse di questo specifico Datacenter in cui ci troviamo
            cl.setResourceParameter(
                    getId(), getCharacteristics().getCostPerSecond(),
                    getCharacteristics().getCostPerBw());

            int userId = cl.getUserId();
            int vmId = cl.getVmId();

            // il tempo necessario per leggere i requiredFiles dal disco
            double fileTransferTime = writeAndPredictTime(cl.getRequiredFiles());

            // TODO: some catch per gli eventuali problemi se la scrittura del file fallisce (disco pieno o altro)

            // troviamo l'host in cui si trova la vm del cloudlet
            Host host = getVmAllocationPolicy().getHost(vmId, userId);
            // get the vm as well
            Vm vm = host.getVm(vmId, userId);
            CloudletScheduler scheduler = vm.getCloudletScheduler();
            // submittiamo il cloudlet, e il metodo ci ritorna il finish time
            double estimatedFinishTime = scheduler.cloudletSubmit(cl, fileTransferTime);

            // if this cloudlet is in the exec queue
            if (estimatedFinishTime > 0.0 && !Double.isInfinite(estimatedFinishTime)) {
                estimatedFinishTime += fileTransferTime;

                // il Datacenter invia a se stesso l'evento generico che lo fa attendere il tempo necessario
                send(getId(), estimatedFinishTime, CloudSimTags.VM_DATACENTER_EVENT);
            }

            if (ack) {
                int[] data = new int[3];
                data[0] = getId();
                data[1] = cl.getCloudletId();
                data[2] = CloudSimTags.TRUE;

                // unique tag = operation tag
                int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
                sendNow(cl.getUserId(), tag, data);
            }
        } catch (ClassCastException c) {
            Log.printLine(getName() + ".processCloudletSubmit(): " + "ClassCastException error.");
            c.printStackTrace();
        } catch (Exception e) {
            Log.printLine(getName() + ".processCloudletSubmit(): " + "Exception error.");
            e.printStackTrace();
        }


        // questo metodo è quello che invia i Cloudlet return
        checkCloudletCompletion();
    }

    /**
     * Write the list of files and predict the total time necessary to perform the operation
     * Mi serve solo per un HDFS block, però per ora lascio la lista di files, penso userò un singolo file che fa da
     * blocco
     *
     * @param requiredFiles the files to be written
     * @return the predicted time
     */
    protected double writeAndPredictTime(List<String> requiredFiles) {
        double time = 0.0;

        Iterator<String> iter = requiredFiles.iterator();
        while (iter.hasNext()) {
            String fileName = iter.next();

            // cycle through all the available drives in the Database
            for (int i = 0; i < getStorageList().size(); i++) {
                // get the drive i
                Storage tempStorage = getStorageList().get(i);

                // TODO: if the drive has enough space and the file is not already present (replica), write it
                // il resto qui sotto è ancora della funzione originale
                File tempFile = tempStorage.getFile(fileName);
                if (tempFile != null) {
                    time += tempFile.getSize() / tempStorage.getMaxTransferRate();
                    break;
                }
            }
        }
        return time;
    }

}
