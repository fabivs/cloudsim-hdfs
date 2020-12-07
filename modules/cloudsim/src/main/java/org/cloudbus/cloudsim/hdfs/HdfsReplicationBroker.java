package org.cloudbus.cloudsim.hdfs;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

// Questo è un normale broker che però non alloca nessuna VM quando viene eseguito, il suo unico ruolo sarà
// di rigirare il cloudlets di replicazione alle vms appropriate
public class HdfsReplicationBroker extends HdfsDatacenterBroker {

    // used only to print prettier logs
    DecimalFormat df = new DecimalFormat("#.###");

    // constructors

    public HdfsReplicationBroker(String name) throws Exception {
        super(name);
    }

    public HdfsReplicationBroker(String name, int cloudletStartId) throws Exception {
        super(name, cloudletStartId);
    }

    @Override
    public void processEvent(SimEvent ev) {
        switch (ev.getTag()) {
            // Resource characteristics request
            case CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST:
                processResourceCharacteristicsRequest(ev);
                break;
            // Resource characteristics answer
            case CloudSimTags.RESOURCE_CHARACTERISTICS:
                processResourceCharacteristics(ev);
                break;
            // VM Creation answer
            case CloudSimTags.VM_CREATE_ACK:
                processVmCreate(ev);
                break;
            // A finished cloudlet returned
            case CloudSimTags.CLOUDLET_RETURN:
                processCloudletReturn(ev);
                break;
            // if the simulation finishes
            case CloudSimTags.END_OF_SIMULATION:
                shutdownEntity();
                break;

            /**
             *  HDFS tags
             */

            // A finished cloudlet returned
            case CloudSimTags.HDFS_CLIENT_CLOUDLET_RETURN:
                processClientCloudletReturn(ev);
                break;
            // A finished DN cloudlet returns, this is used for REPLICATION
            case CloudSimTags.HDFS_DN_CLOUDLET_RETURN:
                processSendReplicaCloudlet(ev);
                break;
            // Il name node ritorna la lista di vms in cui scrivere il blocco
            case CloudSimTags.HDFS_NAMENODE_RETURN_DN_LIST:
                processSendDataCloudlet(ev);
                break;
            // Il Replication Broker ha bisogno di conoscere le VMs e la mappa Vms to Datanodes, questa info gliela manda il broker principale
            case CloudSimTags.HDFS_REP_BROKER_ADD_DN:
                processDataNodeInformation(ev);
                break;

            // other unknown tags are processed by this method
            default:
                processOtherEvent(ev);
                break;
        }
    }

    protected void processDataNodeInformation(SimEvent ev){
        int[] data = (int[]) ev.getData();
        int currentDataNodeId = data[0];
        int currentDatacenterId = data[1];
        int currentRackid = data[2];    // unused
        int currentStorageCapacity = data[3];   // unused

        getVmsToDatacentersMap().put(currentDataNodeId, currentDatacenterId);
        getVmsCreatedList().add(VmList.getById(getVmList(), currentDataNodeId));
    }

    @Override
    protected void submitDNCloudlets() {
        int vmIndex = 0;
        List<Cloudlet> successfullySubmitted = new ArrayList<Cloudlet>();
        for (Cloudlet cloudlet : getCloudletList()) {
            Vm vm;
            // if user didn't bind this cloudlet and it has not been executed yet
            if (cloudlet.getVmId() == -1) {
                vm = getVmsCreatedList().get(vmIndex);
            } else { // submit to the specific vm
                vm = VmList.getById(getVmsCreatedList(), cloudlet.getVmId());
                if (vm == null) { // vm was not created
                    if(!Log.isDisabled()) {
                        Log.printConcatLine(df.format(CloudSim.clock()), ": ", getName(), ": Postponing execution of Data Cloudlet ",
                                cloudlet.getCloudletId(), ": bound VM not available");
                    }
                    continue;
                }
            }

            if (!Log.isDisabled()) {
                Log.printConcatLine(df.format(CloudSim.clock()), ": ", getName(), ": Sending Data Cloudlet ",
                        cloudlet.getCloudletId(), " to VM #", vm.getId());
            }

            // il metodo dovrebbe automaticamente trovare il Datacenter in cui si trova la VM del DN senza problemi
            sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.HDFS_DN_CLOUDLET_SUBMIT, cloudlet);

            cloudletsSubmitted++;
            currentCloudletMaxId = Math.max(cloudlet.getCloudletId(), currentCloudletMaxId);
            vmIndex = (vmIndex + 1) % getVmsCreatedList().size();
            getCloudletSubmittedList().add(cloudlet);
            successfullySubmitted.add(cloudlet);
        }

        // remove submitted cloudlets from waiting list
        getCloudletList().removeAll(successfullySubmitted);
    }

    @Override
    // Identico a processSendDataCloudlet però riceve come evento un HdfsCloudlet intero da rigirare alla prossima destinazione
    protected void processSendReplicaCloudlet(SimEvent ev) {

        HdfsCloudlet originalCloudlet = (HdfsCloudlet) ev.getData();

        Log.printConcatLine(df.format(CloudSim.clock()), ": ", getName(), ": ReplicationCloudlet ", originalCloudlet.getCloudletId(),
                ": the block has been read, sending it to the Data Node...");

        // non molto elegante, ma dovrebbe funzionare lol, da qualche parte sto metodo lo devo prendere
        stagedCloudlet = HdfsCloudlet.cloneCloudletAssignNewId(originalCloudlet, currentCloudletMaxId + 1);

        // store the original vm id, so we can keep track of whose block it is in the DN
        stagedCloudlet.setSourceVmId(originalCloudlet.getVmId());

        // get the destination vms list
        List<Integer> destinationVms = originalCloudlet.getDestVmIds();

        if (destinationVms.isEmpty()){
            Log.printLine(df.format(CloudSim.clock()) + ": " + getName() + ": The replication pipeline is over");
            return;
        }

        // copio il pezzo dal metodo sopra (processClientCloudletReturn()) e ho fatto

        // set the DN VM as the new VM Id for the cloudlet

        stagedCloudlet.setVmId(destinationVms.get(0));

        // ora rimuovo la prima vm dalla lista di destination vms e la aggiungo a destVmIds
        destinationVms.remove(0);
        stagedCloudlet.setDestVmIds(destinationVms);

        // alternativamente si può usare il metodo bind che fa la stessa cosa
        // bindCloudletToVm(cloudlet.getCloudletId(), cloudlet.getVmId());

        // add the cloudlet to the list of submitted cloudlets
        getCloudletList().add(stagedCloudlet);

        // non so se prima settare la VM e poi aggiungere alla CloudletList, o se fare il contrario, vedremo...

        /* ri-eseguiamo questo metodo, che ora troverà il nuovo unbound cloudlet nella lista, e lo invierà
        alla VM appropriata, inoltre settando la posizione del broker uguale a quella del client nella topology,
        avremo una corretta simulazione del delay per l'invio del file tramite network
        */
        submitDNCloudlets();

    }

    @Override
    protected void processResourceCharacteristics(SimEvent ev) {

    }

    @Override
    public void submitVmList(List<? extends Vm> list) {
        getVmList().addAll(list);
        setVmsCreatedList(list);
    }

    @Override
    protected void processVmCreate(SimEvent ev) {
        int[] data = (int[]) ev.getData();
        int datacenterId = data[0];
        int vmId = data[1];
        int result = data[2];

        if (result == CloudSimTags.TRUE) {
            getVmsToDatacentersMap().put(vmId, datacenterId);
            getVmsCreatedList().add(VmList.getById(getVmList(), vmId));
            Log.printConcatLine(df.format(CloudSim.clock()), ": ", getName(), ": VM #", vmId,
                    " has been created in Datacenter#", datacenterId-1, ", Host #",
                    VmList.getById(getVmsCreatedList(), vmId).getHost().getId());
        } else {
            Log.printConcatLine(df.format(CloudSim.clock()), ": ", getName(), ": Creation of VM #", vmId,
                    " failed in Datacenter#", datacenterId-1);
        }

        incrementVmsAcks();

        // all the requested VMs have been created
        if (getVmsCreatedList().size() == getVmList().size() - getVmsDestroyed()) {

            submitCloudlets();
        } else {
            // all the acks received, but some VMs were not created
            if (getVmsRequested() == getVmsAcks()) {
                // find id of the next datacenter that has not been tried
                for (int nextDatacenterId : getDatacenterIdsList()) {
                    if (!getDatacenterRequestedIdsList().contains(nextDatacenterId)) {
                        createVmsInDatacenter(nextDatacenterId);
                        return;
                    }
                }

                // all datacenters already queried
                if (getVmsCreatedList().size() > 0) { // if some vm were created
                    submitCloudlets();
                } else { // no vms created. abort
                    Log.printLine(df.format(CloudSim.clock()) + ": " + getName()
                            + ": none of the required VMs could be created. Aborting");
                    finishExecution();
                }
            }
        }
    }
}
