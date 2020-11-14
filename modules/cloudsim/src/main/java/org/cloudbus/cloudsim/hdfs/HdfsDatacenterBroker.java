package org.cloudbus.cloudsim.hdfs;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;
import org.cloudbus.cloudsim.core.SimEntity;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HdfsDatacenterBroker extends DatacenterBroker {

    protected int currentCloudletMaxId;

    protected int nameNodeId;

    protected HdfsCloudlet stagedCloudlet;

    protected List<Integer> replicationBrokersId;

    /**
     * Created a new DatacenterBroker object. Remember to set the name node as well after creation!
     *
     * @param name name to be associated with this entity (as required by {@link SimEntity} class)
     * @throws Exception the exception
     * @pre name != null
     * @post $none
     */
    public HdfsDatacenterBroker(String name) throws Exception {
        super(name);
        currentCloudletMaxId = 0;
        setReplicationBrokersId(new ArrayList<Integer>());
    }

    // COSTRUTTORE PER I REPLICATION BROKERS, settare il cloudlet max ID a un numero più alto, ad esempio +100
    public HdfsDatacenterBroker(String name, int cloudletStartId) throws Exception {
        super(name);
        currentCloudletMaxId = cloudletStartId;
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

            // other unknown tags are processed by this method
            default:
                processOtherEvent(ev);
                break;
        }
    }

    // Ritorna il cloudlet del client, che ha letto il file
    // Bisogna ora mandare il cloudlet again al DN
    protected void processClientCloudletReturn(SimEvent ev) {

        HdfsCloudlet originalCloudlet = (HdfsCloudlet) ev.getData();

        Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Cloudlet ", originalCloudlet.getCloudletId(),
                ": the block has been read, sending it to the Data Node...");

        // non molto elegante, ma dovrebbe funzionare lol, da qualche parte sto metodo lo devo prendere
        stagedCloudlet = HdfsCloudlet.cloneCloudletAssignNewId(originalCloudlet, currentCloudletMaxId + 1);

        // store the original vm id, so we can keep track of whose block it is in the DN
        stagedCloudlet.setSourceVmId(originalCloudlet.getVmId());

        // now the only thing left to do is to set the list of destination vms, for which the NameNode is needed
        List<String> nameNodeData = new ArrayList<String>();
        nameNodeData.add(originalCloudlet.getRequiredFiles().get(0));
        nameNodeData.add(Integer.toString(originalCloudlet.getReplicaNum()));
        nameNodeData.add(Integer.toString(originalCloudlet.getBlockSize()));
        nameNodeData.add(Integer.toString(getId()));
        sendNow(getNameNodeId(), CloudSimTags.HDFS_NAMENODE_WRITE_FILE, nameNodeData);

        // Il pezzo che era qui è andato ora in processSendDataCloudlet()

    }

    // TODO: questo metodo!
    // Il name node ha ritornato la lista di vms in cui il file deve essere scritto, quindi...
    protected void processSendDataCloudlet(SimEvent ev) {

        // spacchetto ev e prendo la lista di Ids delle vms

        List<Integer> destinationVms = (List<Integer>) ev.getData();

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

    // Identico a processSendDataCloudlet però riceve come evento un HdfsCloudlet intero da rigirare alla prossima destinazione
    protected void processSendReplicaCloudlet(SimEvent ev) {

        HdfsCloudlet originalCloudlet = (HdfsCloudlet) ev.getData();

         Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": ReplicationCloudlet ", originalCloudlet.getCloudletId(),
                ": the block has been read, sending it to the Data Node...");

        // non molto elegante, ma dovrebbe funzionare lol, da qualche parte sto metodo lo devo prendere
        stagedCloudlet = HdfsCloudlet.cloneCloudletAssignNewId(originalCloudlet, currentCloudletMaxId + 1);

        // store the original vm id, so we can keep track of whose block it is in the DN
        stagedCloudlet.setSourceVmId(originalCloudlet.getVmId());

        // get the destination vms list
        List<Integer> destinationVms = originalCloudlet.getDestVmIds();

        if (destinationVms.isEmpty()){
            Log.printLine("The replication pipeline is over");
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


    // deve comunicare (nel caso la vm sia per un client o per DN) al NameNode la vm che è stata creata
    @Override
    protected void processVmCreate(SimEvent ev) {
        int[] data = (int[]) ev.getData();
        int datacenterId = data[0];
        int vmId = data[1];
        int result = data[2];

        if (result == CloudSimTags.TRUE) {
            getVmsToDatacentersMap().put(vmId, datacenterId);
            getVmsCreatedList().add(VmList.getById(getVmList(), vmId));
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": VM #", vmId,
                    " has been created in Datacenter #", datacenterId, ", Host #",
                    VmList.getById(getVmsCreatedList(), vmId).getHost().getId());

            /* PEZZO AGGIUNTO PER HDFS NAME NODE */
            // comunico al name node della Vm creata
            HdfsVm tempVm = VmList.getById(getVmList(), vmId);
            assert tempVm != null;
            int[] tempData;
            // nel caso sia una Client VM
            if (tempVm.getHdfsType() == CloudSimTags.HDFS_CLIENT) {
                tempData = new int[]{tempVm.getId(), getId()};
                sendNow(nameNodeId, CloudSimTags.HDFS_NAMENODE_ADD_CLIENT, tempData);
            }
            // nel caso sia una DN VM
            if (tempVm.getHdfsType() == CloudSimTags.HDFS_DN) {
                HdfsHost tempHost = (HdfsHost) tempVm.getHost();
                // invio al NameNode 4 valori: Id del nodo, Id del datacenter, Id del rack, capacità di storage
                tempData = new int[]{tempVm.getId(), datacenterId, tempHost.getRackId(), (int) tempHost.getProperStorage().getCapacity()};
                // send the information about the DataNodes to the NameNode
                sendNow(nameNodeId, CloudSimTags.HDFS_NAMENODE_ADD_DN, tempData);
                // send the information about the DataNodes to the Replication Brokers
                for (Integer repBrokerId : replicationBrokersId){
                    sendNow(repBrokerId, CloudSimTags.HDFS_REP_BROKER_ADD_DN, tempData);
                }
            }

        } else {
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Creation of VM #", vmId,
                    " failed in Datacenter #", datacenterId);
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
                    Log.printLine(CloudSim.clock() + ": " + getName()
                            + ": none of the required VMs could be created. Aborting");
                    finishExecution();
                }
            }
        }
    }

    /**
     * Submit cloudlets to the created VMs.
     *
     * @pre $none
     * @post $none
     * @see #submitCloudletList(java.util.List)
     */
    @Override
    protected void submitCloudlets() {
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
                        Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Postponing execution of Cloudlet ",
                                cloudlet.getCloudletId(), ": bound VM not available");
                    }
                    continue;
                }
            }

            if (!Log.isDisabled()) {
                Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Sending Cloudlet ",
                        cloudlet.getCloudletId(), " to VM #", vm.getId());
            }

            cloudlet.setVmId(vm.getId());
            sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.HDFS_CLIENT_CLOUDLET_SUBMIT, cloudlet);
            cloudletsSubmitted++;
            currentCloudletMaxId = Math.max(cloudlet.getCloudletId(), currentCloudletMaxId);
            vmIndex = (vmIndex + 1) % getVmsCreatedList().size();
            getCloudletSubmittedList().add(cloudlet);
            successfullySubmitted.add(cloudlet);
        }

        // remove submitted cloudlets from waiting list
        getCloudletList().removeAll(successfullySubmitted);
    }

    // l'unica differenza rispetto a submitCloudlets sta nel tag nella sendNow(..)
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
                        Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Postponing execution of Data Cloudlet ",
                                cloudlet.getCloudletId(), ": bound VM not available");
                    }
                    continue;
                }
            }

            if (!Log.isDisabled()) {
                Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Sending Data Cloudlet ",
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

    public int getNameNodeId() {
        return nameNodeId;
    }

    public void setNameNodeId(int nameNodeId) {
        this.nameNodeId = nameNodeId;
    }

    public List<Integer> getReplicationBrokersId() {
        return replicationBrokersId;
    }

    public void setReplicationBrokersId(List<Integer> replicationBrokersId) {
        this.replicationBrokersId = replicationBrokersId;
    }
}
