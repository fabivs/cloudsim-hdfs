package org.cloudbus.cloudsim.hdfs;

import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;

import java.lang.reflect.Array;
import java.util.*;

public class NameNode extends SimEntity {

    // the list of clients
    protected List<Integer> clientList;

    // maps every client with its own broker (ogni client corrisponde a un broker nella mia simulazione)
    protected Map<Integer, Integer> mapClientToBroker;

    // the NameNode knows about every DataNode (vm)
    protected List<Integer> dataNodeList;

    // each DataNode is also mapped to the Datacenter to which it belongs
    protected Map<Integer, Integer> mapDataNodeToDatacenter;

    // maps every DataNode (vm) ID with the list of blocks it contains as filenames
    protected Map<Integer, List<String>> mapDataNodeToBlocks;

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

        setClientList(new ArrayList<Integer>());
        setMapClientToBroker(new HashMap<Integer, Integer>());

        setDataNodeList(new ArrayList<Integer>());
        setMapDataNodeToDatacenter(new HashMap<Integer, Integer>());
        setMapDataNodeToBlocks(new HashMap<Integer, List<String>>());

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
    // ev contiene: int del client vm id, int del brokerId
    protected void processAddClient(SimEvent ev){

        int[] data = (int[]) ev.getData();
        int currentClientId = data[0];
        int currentBrokerId = data[1];

        Log.printLine(getName() + ": Received client VM of ID " + currentClientId + ", belonging to broker " + currentBrokerId);


        this.clientList.add(currentClientId);
        // aggiunge alla mappa il client id e il corrispondente broker id, necessario per rispedire indietro gli eventi
        this.mapClientToBroker.put(currentClientId, currentBrokerId);
    }

    // adding a new DataNode to the list of current DataNodes
    protected void processAddDataNode(SimEvent ev){

        int[] data = (int[]) ev.getData();
        int currentDataNodeId = data[0];
        int currentDatacenterId = data[1];

        Log.printLine(getName() + ": Received DataNode VM of ID " + currentDataNodeId + ", in Datacenter " + currentDatacenterId);

        this.dataNodeList.add(currentDataNodeId);
        Log.printLine("Top element della datanode list in NameNode: " + getDataNodeList().get(0));
        this.mapDataNodeToDatacenter.put(currentDataNodeId, currentDatacenterId);

    }

    // writing a new File (Block) to the HDFS cluster
    // the NameNode decides in which destination VMs the file and its replicas are supposed to go
    // l'evento ev è un array che contiene: String nome del file, String: preferred number of replicas, String: blocksize
    protected void processWriteFile(SimEvent ev){

        List<String> data = (List<String>) ev.getData();

        String fileName = data.get(0);
        int replicasNumber = Integer.parseInt(data.get(1));
        int blockSize = Integer.parseInt(data.get(2));  // blocksize in MB
        int clientBrokerId = Integer.parseInt(data.get(3));  // ID della client VM che invia

        // nel caso sia undefined, allora il numero di replicas è quello standard del NameNode
        if (replicasNumber == 0) {
            replicasNumber = defaultReplicas;
        }

        // il risultato finale da ritornare al broker
        List<Integer> destinationIds = new ArrayList<Integer>();

        // dobbiamo controllare in quali DataNode VMs il file è già presente

        List<Integer> primaryVms = new ArrayList<Integer>();
        // queste saranno le vms in cui il blocco c'è già, però c'è abbastanza spazio per scriverne un altro
        List<Integer> secondaryVms = new ArrayList<Integer>();

        for (Integer iterDataNode : getMapDataNodeToBlocks().keySet()){
            secondaryVms.add(iterDataNode);
            if (!getMapDataNodeToBlocks().get(iterDataNode).contains(fileName)){
                primaryVms.add(iterDataNode);
                secondaryVms.remove(iterDataNode);
                secondaryVms.add(iterDataNode);     // this data node can still be secondary, but it will be moved at the end of the queue
            }
        }

        // eliminiamo dalla lista di free vms quelle in cui non c'è abbastanza spazio per salvare il blocco
        // nota: vado a controllare nell'host dove è allocata quella vm se c'è abbastanza storage space

        for (Integer currentVm : primaryVms){
            // get the datacenter corresponding to that vm
            HdfsDatacenter currentDataCenter = (HdfsDatacenter) CloudSim.getEntity(getMapDataNodeToDatacenter().get(currentVm));
            // search inside all hosts
            for (Host iterHost : currentDataCenter.getHostList()){
                // for each host, search inside the allocated vms in that host
                for (Vm iterVm : iterHost.getVmList()){
                    // when iterVm is the actual vm we are looking for, it means that this host contains that vm
                    if (iterVm.getId() == currentVm){
                        HdfsHost test = (HdfsHost) iterHost;
                        if (test.getProperStorage().getAvailableSpace() < blockSize)
                            primaryVms.remove(currentVm);
                            secondaryVms.remove(currentVm);
                    }
                }
            }

        }

        Log.printLine("The primary vms are: ");
        for (Integer t : primaryVms)
            Log.print(t + ", ");
        Log.printLine();
        Log.printLine("The secondary vms are: ");
        for (Integer t : secondaryVms)
            Log.print(t + ", ");

        // assegniamo da scrivere il file in tot. DataNodes, quante sono le replicas, in cui il file non c'è già
        // bisogna controllare anche che nel DataNode ci sia enough space per assegnare il file (serve il blocksize lol)

        if (primaryVms.size() >= replicasNumber){
            for (int i = 0; i < replicasNumber; i++){
                destinationIds.add(primaryVms.get(i));
            }
        }

        // se le vms libere sono meno delle repliche da scrivere: usiamo le vms che abbiamo prima,
        // dopodichè usiamo vms in cui c'è almeno abbastanza spazio per scrivere il blocco di nuovo
        if (primaryVms.size() < replicasNumber){
            destinationIds.addAll(primaryVms);  // aggiungiamo tutte le primary vms che abbiamo

            for (int i = 0; i < (replicasNumber - primaryVms.size()); i++){
                if (secondaryVms.isEmpty()){
                    Log.printLine("There are no suitable DataNodes");
                    return;
                }
                destinationIds.add(secondaryVms.get(i));    // usiamo come rimanenti repliche quelle da secondary vms, dove il blocco c'è già, ma almeno c'è spazio
            }
        }

        // aggiungiamo nella hashmap il blocco nei corrispondenti data nodes in cui lo abbiamo allocato
        for (Integer i : destinationIds){
            getMapDataNodeToBlocks().get(i).add(fileName);
        }

        // inviamo indietro al Broker che ce l'ha chiesto, la lista di VMs, che il broker poi infilerà in destVm del Cloudlet (va reimplementata destVM come lista)
        sendNow(clientBrokerId, CloudSimTags.HDFS_NAMENODE_RETURN_DN_LIST, destinationIds);
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
    public List<Integer> getClientList() {
        return clientList;
    }

    @SuppressWarnings("unchecked")
    public void setClientList(List<Integer> clientList) {
        this.clientList = clientList;
    }

    @SuppressWarnings("unchecked")
    public List<Integer> getDataNodeList() {
        return dataNodeList;
    }

    @SuppressWarnings("unchecked")
    public void setDataNodeList(List<Integer> dataNodeList) {
        this.dataNodeList = dataNodeList;
    }

    public Map<Integer, List<String>> getMapDataNodeToBlocks() {
        return mapDataNodeToBlocks;
    }

    public void setMapDataNodeToBlocks(Map<Integer, List<String>> mapDataNodeToBlocks) {
        this.mapDataNodeToBlocks = mapDataNodeToBlocks;
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

    public Map<Integer, Integer> getMapDataNodeToDatacenter() {
        return mapDataNodeToDatacenter;
    }

    public void setMapDataNodeToDatacenter(Map<Integer, Integer> mapDataNodeToDatacenter) {
        this.mapDataNodeToDatacenter = mapDataNodeToDatacenter;
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
