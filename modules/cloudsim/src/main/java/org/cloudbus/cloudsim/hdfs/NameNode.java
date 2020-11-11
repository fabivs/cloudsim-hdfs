package org.cloudbus.cloudsim.hdfs;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NameNode extends SimEntity {

    // the list of clients
    protected List<? extends Vm> clientList;

    // maps every client with its own broker (ogni client corrisponde a un broker nella mia simulazione)
    protected Map<Integer, Integer> mapClientToBroker;

    // the NameNode knows about every DataNode (vm)
    protected List<? extends Vm> dataNodeList;

    // maps every DataNode (vm) ID with the list of blocks it contains as filenames
    protected Map<Integer, List<String>> mapVmToBlocks;

    // the default number of replicas per block
    protected int defaultReplicas;

    // the default size of a block
    // protected int defaultBlockSize;

    /**
     * Creates a new entity.
     *
     * @param name the name to be associated with the entity
     */
    public NameNode(String name, int defaultBlockSize, int defaultReplicas) {
        super(name);

        setClientList(new ArrayList<HdfsVm>());
        setMapClientToBroker(new HashMap<Integer, Integer>());

        setDataNodeList(new ArrayList<HdfsVm>());
        setMapVmToBlocks(new HashMap<Integer, List<String>>());

        // setDefaultBlockSize(defaultBlockSize);
        setDefaultReplicas(defaultReplicas);

    }



    @Override
    public void startEntity() {
        Log.printConcatLine(getName(), " is starting...");
    }

    @Override
    public void processEvent(SimEvent ev) {
        switch (ev.getTag()) {
            // Resource characteristics answer
            case CloudSimTags.HDFS_NAMENODE_ADD_CLIENT:
                processAddClient(ev);
                break;
            // Resource characteristics request
            case CloudSimTags.HDFS_NAMENODE_ADD_DN:
                processAddDataNode(ev);
                break;
            // VM Creation answer
            case CloudSimTags.HDFS_NAMENODE_WRITE_FILE:
                processWriteFile(ev);
                break;
            // if the simulation finishes
            case CloudSimTags.END_OF_SIMULATION:
                shutdownEntity();
                break;
            // other unknown tags are processed by this method
            default:
                processOtherEvent(ev);
                break;
        }
    }

    // adding a new Client to the list of current Clients
    protected void processAddClient(SimEvent ev){



        HdfsVm clientVm = (HdfsVm) ev.getData();
        getClientList().add(clientVm);
    }

    // adding a new DataNode to the list of current DataNodes
    protected void processAddDataNode(SimEvent ev){

        HdfsVm dataVm = (HdfsVm) ev.getData();
        getDataNodeList().add(dataVm);

    }

    // writing a new File (Block) to the HDFS cluster
    // the NameNode decides in which destination VMs the file and its replicas are supposed to go
    // l'evento ev è un array che contiene: String nome del file, String: preferred number of replicas
    protected void processWriteFile(SimEvent ev){

        String[] data = (String[]) ev.getData();

        String fileName = data[0];
        int replicasNumber = Integer.parseInt(data[1]);

        // nel caso sia undefined, allora il numero di replicas è quello standard del NameNode
        if (replicasNumber == 0) {
            replicasNumber = defaultReplicas;
        }

        // dobbiamo controllare in quali DataNodes VMs il file è già presente

        // assegniamo da scrivere il file in tot. DataNodes, quante sono le replicas, in cui il file non c'è già

        // bisogna controllare anche che nel DataNode ci sia enough space per assegnare il file (serve il blocksize lol)

        // in caso non ci dovessero essere data nodes liberi, assegniamo a uno a caso sennò dovremmo dare errore e non va bene

        // inviamo indietro al Broker che ce l'ha chiesto, la lista di VMs, che il broker poi infilerà in destVm del Cloudlet (va reimplementata destVM come lista)

    }

    @Override
    public void shutdownEntity() {
        Log.printConcatLine(getName(), " is shutting down...");
    }

    /**
     * Process non-default received events that aren't processed by
     * the {@link #processEvent(org.cloudbus.cloudsim.core.SimEvent)} method.
     * This method should be overridden by subclasses in other to process
     * new defined events.
     *
     * @param ev a SimEvent object
     * @pre ev != null
     * @post $none
     * @todo to ensure the method will be overridden, it should be defined
     * as abstract in a super class from where new brokers have to be extended.
     */
    protected void processOtherEvent(SimEvent ev) {
        if (ev == null) {
            Log.printConcatLine(getName(), ".processOtherEvent(): ", "Error - an event is null.");
            return;
        }

        Log.printConcatLine(getName(), ".processOtherEvent(): Error - event unknown by this NameNode.");
    }

    // GETTERS AND SETTERS

    @SuppressWarnings("unchecked")
    public <T extends Vm> List<T> getClientList() {
        return (List<T>) clientList;
    }

    @SuppressWarnings("unchecked")
    public <T extends Vm> void setClientList(List<T> clientList) {
        this.clientList = clientList;
    }

    @SuppressWarnings("unchecked")
    public <T extends Vm> List<T> getDataNodeList() {
        return (List<T>) dataNodeList;
    }

    @SuppressWarnings("unchecked")
    public <T extends Vm> void setDataNodeList(List<T> dataNodeList) {
        this.dataNodeList = dataNodeList;
    }

    public Map<Integer, List<String>> getMapVmToBlocks() {
        return mapVmToBlocks;
    }

    public void setMapVmToBlocks(Map<Integer, List<String>> mapVmToBlocks) {
        this.mapVmToBlocks = mapVmToBlocks;
    }

    public int getDefaultReplicas() {
        return defaultReplicas;
    }

    public void setDefaultReplicas(int defaultReplicas) {
        this.defaultReplicas = defaultReplicas;
    }

    public Map<Integer, Integer> getMapClientToBroker() {
        return mapClientToBroker;
    }

    public void setMapClientToBroker(Map<Integer, Integer> mapClientToBroker) {
        this.mapClientToBroker = mapClientToBroker;
    }

    /*
    public int getDefaultBlockSize() {
        return defaultBlockSize;
    }

    public void setDefaultBlockSize(int defaultBlockSize) {
        this.defaultBlockSize = defaultBlockSize;
    }
     */
}
