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
            // A finished cloudlet returned (PROBABILMENTE NON È NECESSARIO, PER ORA È REDUNDANT)
            case CloudSimTags.HDFS_DN_CLOUDLET_RETURN:
                processCloudletReturn(ev);
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
        sendNow(getNameNodeId(), CloudSimTags.HDFS_NAMENODE_WRITE_FILE, nameNodeData);

        // TODO: QUESTO VA CAMBIATO, LA destVm NON È GIÀ NEL CLOUDLET, LA CHIEDIAMO AL NAMENODE (sarà una list)

        // set the DN VM as the new VM Id for the cloudlet
        stagedCloudlet.setVmId(originalCloudlet.getDestVmId());

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

            // non è ridondante questo?
            cloudlet.setVmId(vm.getId());

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
}
