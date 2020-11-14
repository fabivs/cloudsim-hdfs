/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation
 *               of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009, The University of Melbourne, Australia
 */

package org.cloudbus.cloudsim.examples.hdfs;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.hdfs.*;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

import static org.cloudbus.cloudsim.core.CloudSimTags.HDFS_CLIENT;
import static org.cloudbus.cloudsim.core.CloudSimTags.HDFS_DN;
import static org.cloudbus.cloudsim.examples.hdfs.utils.HdfsUtils.*;

public class HdfsExample0 {

	/** The cloudlet list. */
	private static List<Cloudlet> cloudletList;

	/** The vmlist. */
	private static List<HdfsVm> vmList;

	/** The datacenter list */
	private static List<HdfsDatacenter> datacenterList;

	/**
	 * Creates main() to run this example
	 */
	public static void main(String[] args) {

		Log.printLine("Starting HdfsExample0...");

		try {

			// First step: Initialize CloudSim
			int num_user = 4;   // number of cloud users
			Calendar calendar = Calendar.getInstance();
			boolean trace_flag = false;  // means trace events

			CloudSim.init(num_user, calendar, trace_flag);

			// Second step: create the datacenters

			// DATACENTER PARAMETERS

			// values for PEs
			int datacenterPeMips = 1000;		// mips (performance) of a single PE
			int datacenterPeCount = 1;			// number of PEs per Host

			// values for Hosts
			int datacenterHostCount = 9;		// number of Hosts (in totale nel Datacenter)
			int datacenterHostRam = 2048;		// amount of RAM for each Host
			int datacenterHostStorage = 100000;	// amount of Storage assigned to each Host
			int datacenterHostBw = 10000;		// amount of Bandwidth assigned to each Host

			// values for Storage
			int datacenterDiskCount = 9;		// number of Hard Drives in the Datacenter (non è più usato)
			int datacenterDiskSize = 100000;	// capacity of each Hard Drive

			// values for Racks
			int datacenterHostsPerRack = 3;		// amount of Hosts in each Rack of the Datacenter
			int datacenterBaseRackId = 0;

			// create an array with all parameters stored inside
			int[] datacenterParameters = new int[]{datacenterPeMips, datacenterPeCount, datacenterHostCount, datacenterHostRam,
					datacenterHostStorage, datacenterHostBw, datacenterDiskCount, datacenterDiskSize, datacenterHostsPerRack, datacenterBaseRackId};

			int[] datacenterParametersClient = new int[]{datacenterPeMips, datacenterPeCount, datacenterHostCount-8, datacenterHostRam,
					datacenterHostStorage, datacenterHostBw, datacenterDiskCount-8, datacenterDiskSize, datacenterHostsPerRack, datacenterBaseRackId};

			// metto i Datacenters in una list per convenience, in particolare per il metodo printStorageList
			datacenterList =  new ArrayList<HdfsDatacenter>();

			// Client datacenter
			HdfsDatacenter datacenter0 = createDatacenter("Datacenter_0", datacenterParametersClient);
			// Data Nodes datacenter (the starting Cloudlet ID needs to be different)
			HdfsDatacenter datacenter1 = createDatacenterDataNodes("Datacenter_1", 0, datacenterParameters);


			// Third step: Create Brokers

			// CLIENT (Main) BROKER
			HdfsDatacenterBroker broker = createBroker();
			int brokerId = broker.getId();

			// one REPLICATION BROKER for each datacenter for data nodes (ognuno con un nuovo cloudlet base id)
			HdfsReplicationBroker replicationBroker = createBroker(100);
			datacenter1.setReplicationBrokerId(replicationBroker.getId());
			broker.getReplicationBrokersId().add(replicationBroker.getId());

			// creo il NameNode
			int blockSize = 10000;
			int defaultReplicas = 3;
			NameNode nameNode = new NameNode("NameNode1", blockSize, defaultReplicas);

			broker.setNameNodeId(nameNode.getId());

			// Fourth step: Create VMs

			// VM PARAMETERS
			int vmCount = 10;		// number of vms to be created
			int vmMips = 250;		// mips performance of a VM
			int vmPesNumber = 1;	// number of PEs
			int vmRam = 2048;		// vm memory (MB)
			long vmBw = 1000;		// available bandwidth for a VM
			long vmSize = 10000;	// image size (MB)
			String vmm = "Xen";		// name of the Vm manager
			String cloudletSchedulerType = "Time"; // either "Time" shared or "Space" shared

			// NOTE: this will create all identical vms, to create VMs with different parameters, run this method multiple times
			vmList = createVmList(vmCount, brokerId, vmMips, vmPesNumber, vmRam, vmBw, vmSize, vmm, cloudletSchedulerType);

			// TODO: integrare questa parte nel metodo createVmList
			vmList.get(0).setHdfsType(HDFS_CLIENT);
			for (int i = 1; i < vmList.size(); i++)
				vmList.get(i).setHdfsType(HDFS_DN);

			//submit vm list to the broker
			broker.submitVmList(vmList);

			// submit the Data nodes vms to the replication broker
			List<HdfsVm> dnList = new ArrayList<HdfsVm>();

			// only the Data Nodes Vms will be added to the list that is submitted to the replication broker
			for (HdfsVm iterVm : vmList)
				if (iterVm.getHdfsType() == HDFS_DN)
					dnList.add(iterVm);

			replicationBroker.submitVmList(dnList);


			// Fifth step: Create Cloudlets

			cloudletList = new ArrayList<Cloudlet>();

			// CLOUDLET PARAMETERS
			int id = 0;
			long length = 40000;
			long fileSize = 300;
			long outputSize = 300;
			int pesNumber = 1;
			UtilizationModel utilizationModel = new UtilizationModelFull();

			// I'll make two blocks to transfer from vm1 to vm2 and from vm1 to vm3

			// HDFS BLOCKS PARAMETERS
			int blockCount = 3;		// block count deve sempre corrispondere al numero di cloudlets!

			List<File> blockList = createBlockList(blockCount, blockSize);

			// We have to store the files inside the drives of Datacenter 0 first, because the client will read them from there
			datacenter0.addFiles(blockList);	// adds the files in the list as a series of separate files

			// We have to make a list of strings for the "requiredFiles" field inside the HdfsCloudlet constructor
			List<String> blockList1 = new ArrayList<String>();
			blockList1.add(blockList.get(0).getName());

			List<String> blockList2 = new ArrayList<String>();
			blockList2.add(blockList.get(1).getName());

			List<String> blockList3 = new ArrayList<String>();
			blockList3.add(blockList.get(2).getName());

			// Finally we can create the cloudlets
			HdfsCloudlet cloudlet1 = new HdfsCloudlet(id, length, pesNumber, fileSize, outputSize, utilizationModel,
					utilizationModel, utilizationModel, blockList1, blockSize);
			cloudlet1.setUserId(brokerId);

			id++;
			HdfsCloudlet cloudlet2 = new HdfsCloudlet(id, length, pesNumber, fileSize, outputSize, utilizationModel,
					utilizationModel, utilizationModel, blockList2, blockSize);
			cloudlet2.setUserId(brokerId);

			id++;
			HdfsCloudlet cloudlet3 = new HdfsCloudlet(id, length, pesNumber, fileSize, outputSize, utilizationModel,
					utilizationModel, utilizationModel, blockList3, blockSize);
			cloudlet3.setUserId(brokerId);

			// set the destination vm id for the cloudlets
			// queste saranno le VM di destinazione in cui vanno scritti i blocchi HDFS
			// TODO: ovviamente questo ora non dovrebbe più servire, se la vede il NameNode
			//cloudlet1.setDestVmIds(vmList.get(1).getId());	// vm #2
			//cloudlet2.setDestVmIds(vmList.get(2).getId());	// vm #3

			// add the cloudlets to the list
			cloudletList.add(cloudlet1);
			cloudletList.add(cloudlet2);
			cloudletList.add(cloudlet3);

			// submit cloudlet list to the broker
			broker.submitCloudletList(cloudletList);

			// bind the cloudlets to the vms, in questo caso entrambi vanno eseguiti sulla vm1
			// che è la vm del Client che legge i files
			broker.bindCloudletToVm(cloudlet1.getCloudletId(),vmList.get(0).getId());
			broker.bindCloudletToVm(cloudlet2.getCloudletId(),vmList.get(0).getId());


			/*
			// NETWORK TOPOLOGY

			//Sixth step: configure network
			//load the network topology file
			NetworkTopology.buildNetworkTopology("topology.brite");

			//maps CloudSim entities to BRITE entities

			int briteNode=0;
			NetworkTopology.mapNode(datacenter0.getId(),briteNode);

			// TODO: datacenter0 e broker devono trovarsi nello stesso nodo, ma assegnarli allo stesso nodo dà errore, quindi bisogna modificare la topology
			briteNode=1;
			NetworkTopology.mapNode(broker.getId(),briteNode);

			briteNode=3;
			NetworkTopology.mapNode(datacenter1.getId(),briteNode);
			*/


			// Eighth step: Starts the simulation
			CloudSim.startSimulation();

			// Final step: Print results when simulation is over
			List<Cloudlet> newList = broker.getCloudletReceivedList();

			CloudSim.stopSimulation();

        	printCloudletList(newList);

        	// printing the status of the Drives in the Datacenters
        	printStorageList(datacenterList);

			Log.printLine("HdfsExample0 finished!");
		}
		catch (Exception e) {
			e.printStackTrace();
			Log.printLine("The simulation has been terminated due to an unexpected error");
		}
	}


	/**
	 * Creates a Datacenter
	 * @param name name of the datacenter
	 * @param requiredValues an array of 8 integers, which represent, in order:
	 *                       mips performance for a PE, number of PEs,
	 *                       number of Hosts, host RAM, host allocated Storage, host Bandwidth,
	 *                       number of HDDs, size of each HDD
	 * @return the datacenter object
	 * @throws ParameterException
	 */
	private static HdfsDatacenter createDatacenter(String name, int[] requiredValues) throws ParameterException{

		//List<HdfsHost> hostList;
		//List<Pe> peList;

		// values for Pes
		int mips = requiredValues[0];
		int pesNum = requiredValues[1];

		// values for Hosts
		int hostNum = requiredValues[2];
		int hostRam = requiredValues[3];
		int hostStorageSize = requiredValues[4];
		int hostBw = requiredValues[5];

		// values for Storage
		int hddNumber = requiredValues[6]; 	// non serve più perchè faccio un singolo hdd per host
		int hddSize = requiredValues[7];

		int hostsPerRack = requiredValues[8];
		int baseRackId = requiredValues[9];

		// questo metodo, se tutto va bene, mi deve ritornare una lista di Hosts, con Id crescente, ognuno
		// con la propria Pe list (ognuno deve avere una istanza diversa di Pe List)
		List<HdfsHost> hostList = createHostList(hostNum, hostsPerRack, baseRackId, hostRam, hostStorageSize, hostBw, pesNum, mips);

		// DatacenterCharacteristics
		String arch = "x86";			// system architecture
		String os = "Linux";          	// operating system
		String vmm = "Xen";				// virtual machine manager
		double time_zone = 10.0;        // time zone this resource located
		double cost = 3.0;              // the cost of using processing in this resource
		double costPerMem = 0.05;		// the cost of using memory in this resource
		double costPerStorage = 0.001;	// the cost of using storage in this resource
		double costPerBw = 0.0;			// the cost of using bw in this resource

		LinkedList<Storage> storageList = createStorageList(hostList, hddSize);

		DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
				arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);

		// create and return the Datacenter object
		HdfsDatacenter datacenter = null;
		try {
			datacenter = new HdfsDatacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
			datacenterList.add(datacenter);

		} catch (Exception e) {
			e.printStackTrace();
		}

		return datacenter;

	}

	private static HdfsDatacenter createDatacenterDataNodes(String name, int replicationBrokerId, int[] requiredValues) throws ParameterException{

		//List<HdfsHost> hostList;
		//List<Pe> peList;

		// values for Pes
		int mips = requiredValues[0];
		int pesNum = requiredValues[1];

		// values for Hosts
		int hostNum = requiredValues[2];
		int hostRam = requiredValues[3];
		int hostStorageSize = requiredValues[4];
		int hostBw = requiredValues[5];

		// values for Storage
		int hddNumber = requiredValues[6];	// non lo uso più perchè metto un hard drive per host
		int hddSize = requiredValues[7];

		int hostsPerRack = requiredValues[8];
		int baseRackId = requiredValues[9];

		// questo metodo, se tutto va bene, mi deve ritornare una lista di Hosts, con Id crescente, ognuno
		// con la propria Pe list (ognuno deve avere una istanza diversa di Pe List)
		List<HdfsHost> hostList = createHostList(hostNum, hostsPerRack, baseRackId, hostRam, hostStorageSize, hostBw, pesNum, mips);

		// DatacenterCharacteristics
		String arch = "x86";			// system architecture
		String os = "Linux";          	// operating system
		String vmm = "Xen";				// virtual machine manager
		double time_zone = 10.0;        // time zone this resource located
		double cost = 3.0;              // the cost of using processing in this resource
		double costPerMem = 0.05;		// the cost of using memory in this resource
		double costPerStorage = 0.001;	// the cost of using storage in this resource
		double costPerBw = 0.0;			// the cost of using bw in this resource

		LinkedList<Storage> storageList = createStorageList(hostList, hddSize);

		DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
				arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);

		// create and return the Datacenter object
		HdfsDatacenter datacenter = null;
		try {
			datacenter = new HdfsDatacenter(name, replicationBrokerId, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
			datacenterList.add(datacenter);

		} catch (Exception e) {
			e.printStackTrace();
		}

		return datacenter;

	}

	//We strongly encourage users to develop their own broker policies, to submit vms and cloudlets according
	//to the specific rules of the simulated scenario
	private static HdfsDatacenterBroker createBroker(){

		HdfsDatacenterBroker broker = null;
		try {
			broker = new HdfsDatacenterBroker("Broker");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return broker;
	}

	private static HdfsReplicationBroker createBroker(int cloudletBaseId){

		HdfsReplicationBroker broker = null;
		try {
			broker = new HdfsReplicationBroker("ReplicationBroker", cloudletBaseId);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return broker;
	}

}
