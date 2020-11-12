package org.cloudbus.cloudsim.hdfs;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

// Questo è un normale broker che però non alloca nessuna VM quando viene eseguito, il suo unico ruolo sarà
// di rigirare il cloudlets di replicazione alle vms appropriate
public class HdfsReplicationBroker extends HdfsDatacenterBroker {

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

            // other unknown tags are processed by this method
            default:
                processOtherEvent(ev);
                break;
        }
    }

    @Override
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

    @Override
    protected void processResourceCharacteristics(SimEvent ev) {

    }

    @Override
    protected void processResourceCharacteristicsRequest(SimEvent ev) {

    }

    @Override
    protected void processVmCreate(SimEvent ev) {

    }

    @Override
    protected void createVmsInDatacenter(int datacenterId) {

    }
}
