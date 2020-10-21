/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation
 *               of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009, The University of Melbourne, Australia
 */

package org.cloudbus.cloudsim.examples;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.hdfs.HdfsCloudlet;
import org.cloudbus.cloudsim.hdfs.HdfsDatacenter;
import org.cloudbus.cloudsim.hdfs.HdfsDatacenterBroker;
import org.cloudbus.cloudsim.hdfs.HdfsHost;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;


/**
 * A simple example showing how to create
 * a datacenter with two hosts and run two
 * cloudlets on it. The cloudlets run in
 * VMs with different MIPS requirements.
 * The cloudlets will take different time
 * to complete the execution depending on
 * the requested VM performance.
 */
public class HdfsExample0 {

	/** The cloudlet list. */
	private static List<Cloudlet> cloudletList;

	/** The vmlist. */
	private static List<Vm> vmlist;

	/**
	 * Creates main() to run this example
	 */
	public static void main(String[] args) {

		Log.printLine("Starting HdfsExample0...");

		try {
			// First step: Initialize the CloudSim package. It should be called
			// before creating any entities.
			int num_user = 1;   // number of cloud users
			Calendar calendar = Calendar.getInstance();
			boolean trace_flag = false;  // means trace events

			// Initialize the CloudSim library
			CloudSim.init(num_user, calendar, trace_flag);

			// Second step: Create Datacenters
			@SuppressWarnings("unused")
			// Client datacenter
			HdfsDatacenter datacenter0 = createDatacenter("Datacenter_0");
			// Data Nodes datacenter
			HdfsDatacenter datacenter1 = createDatacenter("Datacenter_1");

			//Third step: Create a Broker (ne serve solo uno perchè abbiamo un solo Client)
			HdfsDatacenterBroker broker = createBroker();
			int brokerId = broker.getId();

			//Fourth step: Create one virtual machine
			vmlist = new ArrayList<Vm>();

			//VM description
			int vmId = 0;
			int mips = 250;
			long size = 10000; //image size (MB)
			int ram = 2048; //vm memory (MB)
			long bw = 1000;
			int pesNumber = 1; //number of cpus
			String vmm = "Xen"; //VMM name

			//create three VMs
			Vm vm1 = new Vm(vmId, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());

			vmId++;
			Vm vm2 = new Vm(vmId, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());

			vmId++;
			Vm vm3 = new Vm(vmId, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());

			//add the VMs to the vmList
			vmlist.add(vm1);	// Client
			vmlist.add(vm2);	// Data Node 1
			vmlist.add(vm3);	// Data Node 2

			//submit vm list to the broker
			broker.submitVmList(vmlist);
			// TODO: come posso fare perchè vm1 possa andare solo nel primo datacenter, e vm2 e vm3 per forza nel secondo?

			// TODO: ricorda che i cloudlet devono essere HdfsCloudlets e bisogna assegnarci il requiredFile, che sarebbe il blocco hdfs

			//Fifth step: Create two Cloudlets
			cloudletList = new ArrayList<Cloudlet>();

			//Cloudlet properties, nota che i cloudlets sono identici, a differenza delle VMs
			int id = 0;
			long length = 40000;
			long fileSize = 300;
			long outputSize = 300;
			UtilizationModel utilizationModel = new UtilizationModelFull();

			int blockSize = 10000;

			// I'll make two blocks to transfer from vm1 to vm2 and from vm1 to vm3
			File block1 = new File("block1", blockSize);
			File block2 = new File("block2", blockSize);

			// We have to store the files inside the drives of Datacenter 0 first, because the client will read them from there
			datacenter0.addFile(block1);
			datacenter0.addFile(block2);

			// We have to make a list of strings for the "requiredFiles" field inside the HdfsCloudlet constructor
			List<String> blockList1 = new ArrayList<String>();
			blockList1.add(block1.getName());

			List<String> blockList2 = new ArrayList<String>();
			blockList2.add(block2.getName());

			// Finally we can create the cloudlets
			HdfsCloudlet cloudlet1 = new HdfsCloudlet(id, length, pesNumber, fileSize, outputSize, utilizationModel,
					utilizationModel, utilizationModel, blockList1, blockSize);
			cloudlet1.setUserId(brokerId);

			id++;
			HdfsCloudlet cloudlet2 = new HdfsCloudlet(id, length, pesNumber, fileSize, outputSize, utilizationModel,
					utilizationModel, utilizationModel, blockList2, blockSize);
			cloudlet2.setUserId(brokerId);

			// set the destination vm id for the cloudlets
			// queste saranno le VM di destinazione in cui vanno scritti i blocchi HDFS
			cloudlet1.setDestVmId(vm2.getId());
			cloudlet2.setDestVmId(vm3.getId());

			// add the cloudlets to the list
			cloudletList.add(cloudlet1);
			cloudletList.add(cloudlet2);

			// submit cloudlet list to the broker
			broker.submitCloudletList(cloudletList);

			// bind the cloudlets to the vms, in questo caso entrambi vanno eseguiti sulla vm1
			// che è la vm del Client che legge i files
			broker.bindCloudletToVm(cloudlet1.getCloudletId(),vm1.getId());
			broker.bindCloudletToVm(cloudlet2.getCloudletId(),vm1.getId());

			// Sixth step: Starts the simulation
			CloudSim.startSimulation();

			// Final step: Print results when simulation is over
			List<Cloudlet> newList = broker.getCloudletReceivedList();

			CloudSim.stopSimulation();

        	printCloudletList(newList);

			Log.printLine("CloudSimExample3 finished!");
		}
		catch (Exception e) {
			e.printStackTrace();
			Log.printLine("The simulation has been terminated due to an unexpected error");
		}
	}

	private static HdfsDatacenter createDatacenter(String name) throws ParameterException {

		// 1. Create a list of Hosts inside the Datacenter
		List<HdfsHost> hostList = new ArrayList<HdfsHost>();

		// 2. Each machine has a list of PEs (cores)
		List<Pe> peList = new ArrayList<Pe>();

		int mips = 1000;

		// 3. Create PEs and add them to a list
		// in questo caso abbiamo un singolo core per machine
		peList.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store the Pe id and MIPS Rating

		//4. Create Hosts, each with its own ID and PE list, and add them to the list of machines
		int hostId=0;
		int ram = 2048; //host memory (MB)
		long storageSize = 100000; //host storage
		HarddriveStorage hardDrive = new HarddriveStorage("HDD_0", storageSize);
		int bw = 10000;

		hostList.add(
    			new HdfsHost(
    				hostId,
    				new RamProvisionerSimple(ram),
    				new BwProvisionerSimple(bw),
    				storageSize,
    				hardDrive,
    				peList,
    				new VmSchedulerTimeShared(peList)
    			)
    		); // This is our first machine

		//create another machine

		// ovviamente bisogna creare una nuova peList e aggiungerci un nuovo PE,
		// nota che non fa niente che l'id è sempre 0, probabilmente perchè è relativo al singolo host
		List<Pe> peList2 = new ArrayList<Pe>();
		peList2.add(new Pe(0, new PeProvisionerSimple(mips)));

		// nuova hard drive instance
		HarddriveStorage hardDrive2 = new HarddriveStorage("HDD_1", storageSize);

		hostId++;	// il nuovo host ovviamente non può avere lo stesso id

		hostList.add(
    			new HdfsHost(
    				hostId,
    				new RamProvisionerSimple(ram),
    				new BwProvisionerSimple(bw),
    				storageSize,
    				hardDrive2,
    				peList2,
    				new VmSchedulerTimeShared(peList2)
    			)
    		); // This is our second machine


		// 5. Create a DatacenterCharacteristics object
		String arch = "x86";      // system architecture
		String os = "Linux";          // operating system
		String vmm = "Xen";
		double time_zone = 10.0;         // time zone this resource located
		double cost = 3.0;              // the cost of using processing in this resource
		double costPerMem = 0.05;		// the cost of using memory in this resource
		double costPerStorage = 0.001;	// the cost of using storage in this resource
		double costPerBw = 0.0;			// the cost of using bw in this resource
		LinkedList<Storage> storageList = new LinkedList<Storage>();	//we are not adding SAN devices by now

		// penso che questo sia ovviamente necessario, lol
		storageList.add(hardDrive);
		storageList.add(hardDrive2);

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);

		// 6. Finally, we need to create a PowerDatacenter object.
		HdfsDatacenter datacenter = null;
		try {
			datacenter = new HdfsDatacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
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

	/**
	 * Prints the Cloudlet objects
	 * @param list  list of Cloudlets
	 */
	private static void printCloudletList(List<Cloudlet> list) {
		int size = list.size();
		Cloudlet cloudlet;

		String indent = "    ";
		Log.printLine();
		Log.printLine("========== OUTPUT ==========");
		Log.printLine("Cloudlet ID" + indent + "STATUS" + indent +
				"Data center ID" + indent + "VM ID" + indent + "Time" + indent + "Start Time" + indent + "Finish Time");

		DecimalFormat dft = new DecimalFormat("###.##");
		for (int i = 0; i < size; i++) {
			cloudlet = list.get(i);
			Log.print(indent + cloudlet.getCloudletId() + indent + indent);

			if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS){
				Log.print("SUCCESS");

				Log.printLine( indent + indent + cloudlet.getResourceId() + indent + indent + indent + cloudlet.getVmId() +
						indent + indent + dft.format(cloudlet.getActualCPUTime()) + indent + indent + dft.format(cloudlet.getExecStartTime())+
						indent + indent + dft.format(cloudlet.getFinishTime()));
			}
		}

	}
}
