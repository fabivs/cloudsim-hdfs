package org.cloudbus.cloudsim;

import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;

// L'objective di questo datacenterBroker esteso è di implementare l'evento che riguarda il trasferimento
// di un file, avremo quindi un il case switch per il tag del SimEvent che avviene, e il relativo blocco
// di codice per effettivamente simulare questo file transfer

public class DataCenterBrokerHdfs extends DatacenterBroker{

    /**
     * Created a new DatacenterBroker object.
     *
     * @param name name to be associated with this entity (as required by {@link SimEntity} class)
     * @throws Exception the exception
     * @pre name != null
     * @post $none
     */
    public DataCenterBrokerHdfs(String name) throws Exception {
        super(name);
    }

    protected void moveCloudlet(int datacenterId, int destId){
        String datacenterName = CloudSim.getEntityName(datacenterId);

        sendNow(datacenterId, CloudSimTags.CLOUDLET_MOVE);
    }
}
