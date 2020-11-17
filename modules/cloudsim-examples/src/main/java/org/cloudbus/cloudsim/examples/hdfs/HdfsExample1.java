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
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.hdfs.*;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

import static org.cloudbus.cloudsim.core.CloudSimTags.HDFS_CLIENT;
import static org.cloudbus.cloudsim.core.CloudSimTags.HDFS_DN;
import static org.cloudbus.cloudsim.examples.hdfs.utils.HdfsUtils.*;

public class HdfsExample1 {

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
			int num_user = 6;   // number of cloud users
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

			int[] datacenterParametersClient = new int[]{datacenterPeMips, datacenterPeCount, 1, datacenterHostRam,
					datacenterHostStorage, datacenterHostBw, 1, datacenterDiskSize, datacenterHostsPerRack, datacenterBaseRackId};

			// metto i Datacenters in una list per convenience, in particolare per il metodo printStorageList
			datacenterList =  new ArrayList<HdfsDatacenter>();

			// Client datacenter
			HdfsDatacenter datacenter0 = createDatacenter("Datacenter_0", datacenterParametersClient);
			// Data Nodes datacenter (the starting Cloudlet ID needs to be different)
			HdfsDatacenter datacenter1 = createDatacenterDataNodes("Datacenter_1", 0, datacenterParameters);


			// Third step: Create Brokers

			// CLIENT (Main) BROKER
			HdfsDatacenterBroker broker = createBroker("Broker1");
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
			int vmCount = 11;		// number of vms to be created
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

			// Global broker (delayato)
			GlobalBroker secondBroker = new GlobalBroker("SecondBroker", 800, 3, 3, nameNode.getId(), datacenter0);
			GlobalBroker thirdBroker = new GlobalBroker("ThirdBroker", 1600, 6, 5, nameNode.getId(), datacenter0);

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
			utilizationModel = new UtilizationModelFull();
			HdfsCloudlet cloudlet2 = new HdfsCloudlet(id, length, pesNumber, fileSize, outputSize, utilizationModel,
					utilizationModel, utilizationModel, blockList2, blockSize);
			cloudlet2.setUserId(brokerId);

			id++;
			utilizationModel = new UtilizationModelFull();
			HdfsCloudlet cloudlet3 = new HdfsCloudlet(id, length, pesNumber, fileSize, outputSize, utilizationModel,
					utilizationModel, utilizationModel, blockList3, blockSize);
			cloudlet3.setUserId(brokerId);

			// set a different number of replica per each file
			cloudlet1.setReplicaNum(5);
			cloudlet2.setReplicaNum(5);
			cloudlet3.setReplicaNum(5);

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
	private static HdfsDatacenterBroker createBroker(String name){

		HdfsDatacenterBroker broker = null;
		try {
			broker = new HdfsDatacenterBroker(name);
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

	// GlobalBroker is used to delay the execution of the broker

	public static class GlobalBroker extends SimEntity {

		private static final int CREATE_BROKER = 0;
		private List<HdfsVm> vmList;
		private List<HdfsCloudlet> cloudletList;
		private HdfsDatacenterBroker broker;
		private HdfsDatacenter clientDatacenter;
		private int nameNodeId;
		private int delay;
		private int baseBlockIndex;
		private int replicaNum;

		public GlobalBroker(String name, int delay, int baseBlockIndex, int replicaNum, int nameNodeId, HdfsDatacenter clientDatacenter) {
			super(name);
			this.clientDatacenter = clientDatacenter;
			this.nameNodeId = nameNodeId;
			this.delay = delay;
			this.baseBlockIndex = baseBlockIndex;
			this.replicaNum = replicaNum;
		}

		@Override
		public void processEvent(SimEvent ev) {
			switch (ev.getTag()) {
				case CREATE_BROKER:

					printStorageList(datacenterList);

					setBroker(createBroker(super.getName()+"_"));

					broker.setNameNodeId(nameNodeId);

					// VM PARAMETERS
					int vmCount = 11;		// number of vms to be created
					int vmMips = 250;		// mips performance of a VM
					int vmPesNumber = 1;	// number of PEs
					int vmRam = 2048;		// vm memory (MB)
					long vmBw = 1000;		// available bandwidth for a VM
					long vmSize = 10000;	// image size (MB)
					String vmm = "Xen";		// name of the Vm manager
					String cloudletSchedulerType = "Time"; // either "Time" shared or "Space" shared

					// NOTE: this will create all identical vms, to create VMs with different parameters, run this method multiple times
					vmList = createVmList(vmCount, broker.getId(), vmMips, vmPesNumber, vmRam, vmBw, vmSize, vmm, cloudletSchedulerType);

					// TODO: integrare questa parte nel metodo createVmList
					vmList.get(0).setHdfsType(HDFS_CLIENT);
					for (int i = 1; i < vmList.size(); i++)
						vmList.get(i).setHdfsType(HDFS_DN);

					setVmList(vmList);	// prendiamo le VMs che già esistono

					// dobbiamo creare ora nuovi cloudlets con nuovi blocchi

					cloudletList = new ArrayList<HdfsCloudlet>();

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
					int blockSize = 10000;

					List<File> blockList = null;
					try {
						blockList = createBlockList(blockCount, blockSize, baseBlockIndex);
					} catch (ParameterException e) {
						e.printStackTrace();
					}

					// We have to store the files inside the drives of Datacenter 0 first, because the client will read them from there
					clientDatacenter.addFiles(blockList);	// adds the files in the list as a series of separate files

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
					cloudlet1.setUserId(broker.getId());

					id++;
					utilizationModel = new UtilizationModelFull();
					HdfsCloudlet cloudlet2 = new HdfsCloudlet(id, length, pesNumber, fileSize, outputSize, utilizationModel,
							utilizationModel, utilizationModel, blockList2, blockSize);
					cloudlet2.setUserId(broker.getId());

					id++;
					utilizationModel = new UtilizationModelFull();
					HdfsCloudlet cloudlet3 = new HdfsCloudlet(id, length, pesNumber, fileSize, outputSize, utilizationModel,
							utilizationModel, utilizationModel, blockList3, blockSize);
					cloudlet3.setUserId(broker.getId());

					cloudlet1.setReplicaNum(replicaNum);
					cloudlet2.setReplicaNum(replicaNum);
					cloudlet3.setReplicaNum(replicaNum);

					// add the cloudlets to the list
					cloudletList.add(cloudlet1);
					cloudletList.add(cloudlet2);
					cloudletList.add(cloudlet3);

					setCloudletList(cloudletList);

					broker.submitVmList(getVmList());
					broker.submitCloudletList(getCloudletList());

					broker.bindCloudletToVm(cloudlet1.getCloudletId(),vmList.get(0).getId());
					broker.bindCloudletToVm(cloudlet2.getCloudletId(),vmList.get(0).getId());

					CloudSim.resumeSimulation();

					break;

				default:
					Log.printLine(getName() + ": unknown event type");
					break;
			}
		}

		// questo è quello che avviene quando la simulazione avvia questo broker
		@Override
		public void startEntity() {
			Log.printLine(super.getName()+" is starting...");
			schedule(getId(), delay, CREATE_BROKER);
		}

		@Override
		public void shutdownEntity() {
		}

		public List<HdfsVm> getVmList() {
			return vmList;
		}

		protected void setVmList(List<HdfsVm> vmList) {
			this.vmList = vmList;
		}

		public List<HdfsCloudlet> getCloudletList() {
			return cloudletList;
		}

		protected void setCloudletList(List<HdfsCloudlet> cloudletList) {
			this.cloudletList = cloudletList;
		}

		public DatacenterBroker getBroker() {
			return broker;
		}

		protected void setBroker(HdfsDatacenterBroker broker) {
			this.broker = broker;
		}

	}

}
