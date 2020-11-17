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

    // maps every DataNode (vm) ID with the associated Rack ID in its own Datacenter
    protected Map<Integer, Integer> mapDataNodeToRackId;

    // maps every DataNode (vm) ID con la max storage capacity che ha
    protected Map<Integer, Integer> mapDataNodeToCapacity;

    // maps every node with its filling %
    protected Map<Integer, Double> mapDataNodeToUsage;

    // maps every rack with its filling %
    protected Map<Integer, Double> mapRackToUsage;

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
        setMapDataNodeToRackId(new HashMap<Integer, Integer>());
        setMapDataNodeToCapacity(new HashMap<Integer, Integer>());
        setMapDataNodeToUsage(new HashMap<Integer, Double>());

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
        //for (Integer i : getClientList())
        //    Log.printLine("Lista di Clients in NameNode: " + i);
        // aggiunge alla mappa il client id e il corrispondente broker id, necessario per rispedire indietro gli eventi
        this.mapClientToBroker.put(currentClientId, currentBrokerId);
    }

    // adding a new DataNode to the list of current DataNodes
    protected void processAddDataNode(SimEvent ev){

        int[] data = (int[]) ev.getData();
        int currentDataNodeId = data[0];
        int currentDatacenterId = data[1];
        int currentRackid = data[2];
        int currentStorageCapacity = data[3];

        Log.printLine(getName() + ": Received DataNode VM of ID " + currentDataNodeId + ", in Datacenter " + currentDatacenterId);

        // Aggiungiamo il nodo alla lista di DataNodes
        this.dataNodeList.add(currentDataNodeId);
        //for (Integer i : getDataNodeList())
        //    Log.printLine("Lista di DataNodes in NameNode: " + i);

        // Mappiamo il nodo al data center
        this.mapDataNodeToDatacenter.put(currentDataNodeId, currentDatacenterId);
        //Log.printLine("=== TEST: la mappa di DNs e Datacenters " + getMapDataNodeToDatacenter());

        // Mappiamo il nodo al rack
        this.mapDataNodeToRackId.put(currentDataNodeId, currentRackid);

        // Mappiamo il nodo alla sua capacità massima
        this.mapDataNodeToCapacity.put(currentDataNodeId, currentStorageCapacity);

        // Settiamo la % di utilizzo del nodo iniziale, che è 0%
        this.mapDataNodeToUsage.put(currentDataNodeId, 0.0);

        // Se il rack non è già presente setto il suo utilizzo a 0% ma NON lo faccio più, perchè mi servirebbe anche un'altra mappa che mappa
        // ogni rack a un datacenter, sennò ovviamente i rack id si sovrapporrebbero, lasciamo stare, me lo trovo a mano quando mi serve questo valore
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

        Log.printLine("NameNode ha ricevuto una richiesta di scrittura, nome del file: " + fileName + ", repliche: " + replicasNumber + ", block size: " + blockSize + ", da parte del client: " + clientBrokerId);

        // nel caso sia undefined, allora il numero di replicas è quello standard del NameNode
        if (replicasNumber == 0) {
            replicasNumber = defaultReplicas;
        }

        // il risultato finale da ritornare al broker
        List<Integer> destinationIds = new ArrayList<Integer>();

        List<Integer> acceptableDestinations = new ArrayList<Integer>();

        // IL DATANODE NON PUÒ GIÀ CONTENERE IL BLOCCO, SE LO CONTIENE GIÀ È UN RIFIUTO SECCO.
        // IL PRIMO BLOCCO LO SCRIVIAMO NEL NODO CON MINORE % DI RIEMPIMENTO (GIUSTIFICAZIONE: TUTTI I NODI SONO EQUIDISTANTI DAI CLIENT)
        // PER IL SECONDO BLOCCO: CONSIDERO TUTTI GLI ALTRI RACKS, SCELGO IL RACK CON MENO % DI RIEMPIMENTO
        // ALL'INTERNO DI QUESTO RACK SCELGO I DUE NODI CON MENO % DI RIEMPIMENTO

        // iniziamo escludendo tutti i DataNodes che già contengono il blocco
        for (Integer iterDataNode : getDataNodeList()){
            if (!getMapDataNodeToBlocks().containsKey(iterDataNode)) {   // se il DN non è presente nella hash map, vuol dire che il nodo è vuoto
                acceptableDestinations.add(iterDataNode);
            } else if (!getMapDataNodeToBlocks().get(iterDataNode).contains(fileName)){
                acceptableDestinations.add(iterDataNode);
            }
        }

        if (acceptableDestinations.isEmpty()){
            Log.print("No suitable nodes were found to write the block to!");
            return;
        }

        // prendiamo il nodo tra quelli candidati in cui la % di utilizzo è minima (questo non è true HDFS)
        // così abbiamo scelto la destination della prima replica
        double minUsage = 999.9;
        Integer firstNode = null;

        for (Integer tempNode : acceptableDestinations){
            if (getMapDataNodeToUsage().get(tempNode) < minUsage){
                firstNode = tempNode;
                minUsage = getMapDataNodeToUsage().get(tempNode);
            }
        }

        // il nodo scelto lo aggiungo alla lista di risultati e lo rimuovo dalle destinations candidate
        destinationIds.add(firstNode);
        acceptableDestinations.remove(firstNode);
        replicasNumber--;   // ho bisogno di sapere quante repliche restano da scrivere per il prossimo ciclo

        // ora mancano gli altri nodi che vanno in rack remoti

        // per iniziare sono accettabili tutti i racks tranne quello della prima destinazione
        Set<Integer> acceptableRacks = new HashSet<Integer>(getMapDataNodeToRackId().values());
        acceptableRacks.remove(getMapDataNodeToRackId().get(firstNode));

        double currentMinRackUsage;

        // 2 nodi max per rack, fino a esaurimento repliche
        // scegliamo il rack con meno overall usage e con almeno due nodi che fanno parte di quelli accettabili
        double cycles = replicasNumber / (double) 2;
        cycles = (int) Math.ceil(cycles);
        for (int i = 1; i <= cycles; i++){

            int validNodesPerRack = 0;
            List<Integer> originalAcceptableRacks = new ArrayList<Integer>(acceptableRacks);

            // prima di tutto tolgo da acceptableRacks i rack che non hanno almeno 2 nodi accettabili
            for (Integer rack : originalAcceptableRacks){
                for (Integer node : acceptableDestinations){
                    if (getMapDataNodeToRackId().get(node).equals(rack)){
                        validNodesPerRack++;
                    }
                }
                if (validNodesPerRack < 2){
                    acceptableRacks.remove(rack);
                }
            }

            // ora cerco il rack tra quelli accettabili con usage ratio minore (questo non è true HDFS)
            currentMinRackUsage = 999.9;
            Integer chosenRack = null;

            for (Integer rack : acceptableRacks){
                if (findRackOverallUsage(rack) < currentMinRackUsage){
                    chosenRack = rack;
                    currentMinRackUsage = findRackOverallUsage(rack);
                }
            }

            acceptableRacks.remove(chosenRack);

            // all'interno di questo rack scelgo i due nodi con usage ratio minore

            Integer previousNode = null;
            Integer chosenNode;

            for (int k = 0; k < 2; k++){

                double minNodeUsage = 999.9;
                chosenNode = null;

                for (Integer tempNode : acceptableDestinations){
                    if (getMapDataNodeToUsage().get(tempNode) < minNodeUsage && getMapDataNodeToRackId().get(tempNode).equals(chosenRack) && !tempNode.equals(previousNode)){
                        chosenNode = tempNode;
                        minNodeUsage = getMapDataNodeToUsage().get(tempNode);
                    }
                }

                // il nodo scelto lo aggiungo alla lista di risultati e lo rimuovo dalle destinations candidate
                if (chosenNode != null){
                    previousNode = chosenNode;
                    destinationIds.add(chosenNode);
                    acceptableDestinations.remove(chosenNode);
                }
            }

        }

        // devo cambiare le % di utilizzo dei nodi che ho settato!!
        updateNodeUsage(destinationIds, blockSize);

        // aggiungiamo nella hashmap il blocco nei corrispondenti data nodes in cui lo abbiamo allocato
        for (Integer i : destinationIds){
            if (getMapDataNodeToBlocks().get(i) == null)
                getMapDataNodeToBlocks().put(i, new ArrayList<String>(Collections.singleton(fileName)));
            else
                getMapDataNodeToBlocks().get(i).add(fileName);
        }

        // inviamo indietro al Broker che ce l'ha chiesto, la lista di VMs, che il broker poi infilerà in destVm del Cloudlet (va reimplementata destVM come lista)
        sendNow(clientBrokerId, CloudSimTags.HDFS_NAMENODE_RETURN_DN_LIST, destinationIds);
    }

    protected double findRackOverallUsage(Integer rackId){
        double usage = 0.0;

        int totalCapacity = 0;
        double totalSpaceUsed = 0.0;

        for (Integer i : getMapDataNodeToRackId().keySet()){
            if (getMapDataNodeToRackId().get(i).equals(rackId)){
                totalCapacity += getMapDataNodeToCapacity().get(i);
                totalSpaceUsed += ( getMapDataNodeToUsage().get(i) * getMapDataNodeToCapacity().get(i));
            }
        }

        return (totalSpaceUsed / totalCapacity);
    }

    protected void updateNodeUsage (List<Integer> nodesToUpdate, int blockSize){

        double currentNodeUsage;
        double currentNodeCapacity;
        double currentNodeStorageAmount;
        double currentNodeUpdatedUsage;

        for (Integer currentNode : nodesToUpdate){
            currentNodeUsage = getMapDataNodeToUsage().get(currentNode);
            currentNodeCapacity = getMapDataNodeToCapacity().get(currentNode);
            currentNodeStorageAmount = (currentNodeUsage * currentNodeCapacity);

            currentNodeUpdatedUsage = (currentNodeStorageAmount + blockSize) / currentNodeCapacity;
            this.mapDataNodeToUsage.put(currentNode, currentNodeUpdatedUsage);
        }
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

    public Map<Integer, Integer> getMapDataNodeToRackId() {
        return mapDataNodeToRackId;
    }

    public void setMapDataNodeToRackId(Map<Integer, Integer> mapDataNodeToRackId) {
        this.mapDataNodeToRackId = mapDataNodeToRackId;
    }

    public Map<Integer, Integer> getMapDataNodeToCapacity() {
        return mapDataNodeToCapacity;
    }

    public void setMapDataNodeToCapacity(Map<Integer, Integer> mapDataNodeToCapacity) {
        this.mapDataNodeToCapacity = mapDataNodeToCapacity;
    }

    public Map<Integer, Double> getMapDataNodeToUsage() {
        return mapDataNodeToUsage;
    }

    public void setMapDataNodeToUsage(Map<Integer, Double> mapDataNodeToUsage) {
        this.mapDataNodeToUsage = mapDataNodeToUsage;
    }

    public Map<Integer, Double> getMapRackToUsage() {
        return mapRackToUsage;
    }

    public void setMapRackToUsage(Map<Integer, Double> mapRackToUsage) {
        this.mapRackToUsage = mapRackToUsage;
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
