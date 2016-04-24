import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupResult;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusResult;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsRequest;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStatus;
import com.amazonaws.services.ec2.model.InstanceStatusSummary;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;

	/*File Header:
	 Name: Rajat Kapoor			Andrew ID: rajatk
	 This file contains the program required to run the Horizontal Scaling
	 task for Project 2.1*/

public class HorizontalScaling {

	private String propertyFile = "/AwsCredentials.properties";
	private Regions regions = Regions.US_EAST_1;
	private String endPoint = "ec2.us-east-1.amazonaws.com";
	private String securityGroupName = "rajatk_P2.1_SG";
	private String securityGroupDesc = "Project2.1 security group";
	private String securityGroupId = null;
	private String tagKey = "Project";
	private String tagValue = "2.1";
	private String loadGenImgId = "ami-4c4e0f24";
	private String dataCenterImgId = "ami-b04106d8";

	/*Function Header:
	The readCredentials() function is used to read from the AwsCredentials
	properties file and return a BasicAWSCredentials object which can be
	used later in the file*/
	
	public BasicAWSCredentials readCredentials() {
		Properties properties = new Properties();

		try { // read the properties file containing credentials
			properties.load(HorizontalScaling.class
					.getResourceAsStream(this.propertyFile));
		} catch (FileNotFoundException e) {
			System.out.println("AwsCredentials.properties file not found!");
			e.printStackTrace(); // return error if file is not found
		} catch (IOException e) {
			e.printStackTrace(); // return error if there are any I/O
									// problems in reading the file
		}

		// read credentials from the properties file and save them in
		// BasicAWSCredentials object:

		BasicAWSCredentials bawsc = new BasicAWSCredentials(
				properties.getProperty("accessKey"),
				properties.getProperty("secretKey"));
		return bawsc;
	}
	
	/*Function Header:
	The instantiateEC2Client() function is used to instantiate an EC2Client
	object and return it. It also sets the region and endpoint of all further
	EC2 instances created in the program*/
	
	public AmazonEC2Client instantiateEC2Client(BasicAWSCredentials credentials) {
		// Create an Amazon EC2 Client
		AmazonEC2Client ec2 = new AmazonEC2Client(credentials);
		// Set region and endpoint
		Region usEast1 = Region.getRegion(this.regions);
		ec2.setRegion(usEast1);
		ec2.setEndpoint(this.endPoint);
		return ec2;
	}
	
	/*Function Header:
	The createSecurityGroup function is used to check if an appropriate
	security group which allows HTTP requests is created or not. If the
	required security group is not already created, it creates one with
	a specified name*/
	
	public void createSecurityGroup(BasicAWSCredentials credentials,
			AmazonEC2Client ec2) {
		// Create a security group to be used for this project
		try {
			CreateSecurityGroupRequest csgr = new CreateSecurityGroupRequest();
			csgr.withGroupName(this.securityGroupName)
					.withDescription(this.securityGroupDesc)
					.setRequestCredentials(credentials);
			CreateSecurityGroupResult csgres = ec2.createSecurityGroup(csgr);

			// set permissions on this security group_
			IpPermission ipPermission = new IpPermission();

			ipPermission.withIpRanges("0.0.0.0/0").withIpProtocol("tcp")
					.withFromPort(80).withToPort(80);

			AuthorizeSecurityGroupIngressRequest authorizeSecurityGroupIngressRequest = new AuthorizeSecurityGroupIngressRequest();

			authorizeSecurityGroupIngressRequest.withGroupName(
					this.securityGroupName).withIpPermissions(ipPermission);

			ec2.authorizeSecurityGroupIngress(authorizeSecurityGroupIngressRequest);
		} catch (AmazonServiceException ase) {
			// if the security group already exists, continue
			// the program
		}
	}
	
	/*Function Header:
	The spawnInstance function is used to create any EC2 instances required.
	The main parameters passed in this function are imageId which may be a
	public image id, instanceType such as t1.micro, m3.medium, etc. and
	subnetId which is used so that all instances created are in the same
	subnet for improved latency. The subnetId is passed as null for the
	load generator instance because its own subnetId is used as a reference
	when creating other instances. This function also assigns the relevant
	security group to the instances and tags them.*/
	
	public Instance spawnInstance(AmazonEC2Client ec2, String imageId,
			String instanceType, String subnetId) {
		// Create a securityGroup array that is to be
		// set on the instance
		String[] securityGroupsIds = new String[1];
		securityGroupsIds[0] = this.securityGroupId;

		// Create Instance Request
		RunInstancesRequest runInstancesRequest = new RunInstancesRequest();

		// Configure Instance Request
		runInstancesRequest.withImageId(imageId).withInstanceType(instanceType)
				.withMinCount(new Integer(1)).withMaxCount(new Integer(1))
				.withSecurityGroupIds(securityGroupsIds);

		if (subnetId != null)
			runInstancesRequest.withSubnetId(subnetId);

		// Launch Instance
		RunInstancesResult runInstancesResult = ec2
				.runInstances(runInstancesRequest);

		// get the instance object generated
		Instance instance = runInstancesResult.getReservation().getInstances()
				.get(0);

		// Set tags on instance
		ArrayList<Tag> requestTags = new ArrayList<Tag>();
		requestTags.add(new Tag(this.tagKey, this.tagValue));
		CreateTagsRequest createTagsRequest_requests = new CreateTagsRequest();
		ArrayList<String> instanceIds = new ArrayList<String>();
		instanceIds.add(instance.getInstanceId());
		createTagsRequest_requests.setResources(instanceIds);
		createTagsRequest_requests.setTags(requestTags);
		ec2.createTags(createTagsRequest_requests);

		return instance;
	}
	
	/*Function header:
	The waitForInstanceRunningState function is used to hold execution
	until an instance passed as parameter has a running state and 2/2
	status checks passed. This is useful in ensuring that instances are
	live before they are used*/
	
	public void waitForInstanceRunningState(AmazonEC2Client ec2,
			Instance instance) {
		// Reference:
		// http://stackoverflow.com/questions/8828964/getting-state-of-ec2-instance-java-api
		ArrayList<String> instanceIds = new ArrayList<String>();
		instanceIds.add(instance.getInstanceId());
		DescribeInstanceStatusRequest dir = new DescribeInstanceStatusRequest()
				.withInstanceIds(instanceIds);
		DescribeInstanceStatusResult describeInstanceResult = ec2
				.describeInstanceStatus(dir);
		List<InstanceStatus> state = describeInstanceResult
				.getInstanceStatuses();
		InstanceStatusSummary status = null;

		while (state.size() < 1) {
			describeInstanceResult = ec2.describeInstanceStatus(dir);
			state = describeInstanceResult.getInstanceStatuses();
		}
		while (status == null || status.getStatus().equals("initializing")) {
			describeInstanceResult = ec2.describeInstanceStatus(dir);
			state = describeInstanceResult.getInstanceStatuses();
			status = state.get(0).getInstanceStatus();
		}
	}
	
	/*Function header:
	The findSecGrpId() function is used to retrieve the group ID
	of the security group that this program uses. The group ID is
	required in spawnInstance() instance function defined earlier
	because new instances that need to be created within the same
	subnet should use group ID for referencing security groups and
	not group names.*/
	
	public void findSecGrpId(AmazonEC2Client ec2) {
		DescribeSecurityGroupsRequest dsgr = new DescribeSecurityGroupsRequest();
		dsgr.withGroupNames(this.securityGroupName);

		DescribeSecurityGroupsResult dsgres = ec2.describeSecurityGroups(dsgr);
		if (dsgres.getSecurityGroups().size() > 0)
			this.securityGroupId = dsgres.getSecurityGroups().get(0)
					.getGroupId();
	}
	
	/*Function Header:
	The getInstancePublicDnsName function is used to retrive the public
	DNS names of instances. This is required separately because instances
	are not assigned a public DNS until they have a running state. Thus,
	this function is called to find the public DNS name of instances after
	they run*/
	
	public String getInstancePublicDnsName(AmazonEC2Client ec2,
			String instanceId) {
		// Ref:
		// http://stackoverflow.com/questions/9241584/get-public-dns-of-amazon-ec2-instance-from-java-api

		DescribeInstancesResult describeInstancesRequest = ec2
				.describeInstances();
		List<Reservation> reservations = describeInstancesRequest
				.getReservations();
		for (Reservation reservation : reservations) {
			for (Instance instance : reservation.getInstances()) {
				if (instance.getInstanceId().equals(instanceId))
					return instance.getPublicDnsName();
			}
		}
		return null;
	}

	public static void main(String[] args) throws IOException,
			InterruptedException {
		BasicAWSCredentials credentials = null;
		AmazonEC2Client ec2 = null;
		Instance loadGeneratorInstance = null;
		String loadGeneratorSubnet = null;
		Instance dataCenterInstance = null;
		String loadGenDNS = null;
		String dataCenterDNS = null;
		float totalRps = 0;
		
		//Instantiate the class object for calling class methods
		
		HorizontalScaling hs = new HorizontalScaling();
		System.out.println("Reading credentials..");
		credentials = hs.readCredentials();

		System.out.println("Instantiating EC2 client..");
		ec2 = hs.instantiateEC2Client(credentials);

		System.out.println("Checking security group..");
		hs.createSecurityGroup(credentials, ec2);

		System.out.println("Finding security group ID..");
		hs.findSecGrpId(ec2); // used to initialize the security group ID
							 // which is used when spawning new datacenter
							// instances in the same subnet

		System.out.println("Spawning Load Generator instance..");
		loadGeneratorInstance = hs.spawnInstance(ec2, hs.loadGenImgId,
				"m3.medium", null);

		loadGeneratorSubnet = loadGeneratorInstance.getSubnetId();

		System.out.println("Spawning initial data center..");
		dataCenterInstance = hs.spawnInstance(ec2, hs.dataCenterImgId,
				"m3.medium", loadGeneratorSubnet);

		System.out.println("Waiting for instances to enter running mode..");
		hs.waitForInstanceRunningState(ec2, loadGeneratorInstance);
		hs.waitForInstanceRunningState(ec2, dataCenterInstance);

		System.out.println("Finding public DNS of Load generator..");
		loadGenDNS = hs.getInstancePublicDnsName(ec2,
				loadGeneratorInstance.getInstanceId());
		System.out.println("Finding public DNS of initial data center..");
		dataCenterDNS = hs.getInstancePublicDnsName(ec2,
				dataCenterInstance.getInstanceId());

		String loadGenURL = "http://" + loadGenDNS + "/test/horizontal?dns="
				+ dataCenterDNS;

		System.out.println("Horizontal scale test initialized..");

		String testId = null;
		String line = null;
		StringBuilder sb = new StringBuilder();
		String testIdPattern = "(test.)([0-9]+)(.log)";	//to extract testId

		try {
			URL url = new URL(loadGenURL);		//open up the load generator URL
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("GET");
			int responseCode = con.getResponseCode();
			if (responseCode == 200) {		//if response is OK
				BufferedReader in = new BufferedReader(new InputStreamReader(
						con.getInputStream()));
				line = in.readLine();
				in.close();

				Pattern testPat = Pattern.compile(testIdPattern);	//search for testId
				Matcher testMatch = testPat.matcher(line);
				if (testMatch.find())
					testId = testMatch.group(2);
				String testLogURL = "http://" + loadGenDNS + "/log?name=test."
						+ testId + ".log";
				url = new URL(testLogURL);

				for (int i = 1; i < 10; i++) { // 10 is the max no. of data
												// centers
												// that can be launched
					con = (HttpURLConnection) url.openConnection();
					responseCode = con.getResponseCode();	//open up the test log URL
					if (responseCode == 200) {
						in = new BufferedReader(new InputStreamReader(
								con.getInputStream()));
						while ((line = in.readLine()) != null)
							sb.append(line);
						Pattern rpsPat = Pattern	//search for rps pattern
								.compile("(.*?)(=)([0-9]+.[0-9]+)(.*?)");
						String toMatch = null;
						if (sb.toString().contains("[Minute "))
							toMatch = sb.toString().substring(
									sb.lastIndexOf("[Minute "));
						else
							toMatch = "";
						Matcher rpsMatch = rpsPat.matcher(toMatch);
						totalRps = 0;	//at every read totalRps is initialized
										//to zero because we only read the last
										//Minute rps and add them together
						while (rpsMatch.find()) {
							String currentRps = rpsMatch.group(3);
							totalRps += Float.parseFloat(currentRps);
						}
						System.out.println("Last read total RPS=" + totalRps);
						if (totalRps > 4000) {	//Exit the program if target RPS is achieved
							System.out
									.println("4000 RPS achieved! Quitting program..");
							System.exit(0);
						}
					}

					try {
						Thread.sleep(100000);	//Before spawning a new instance
												//wait for 100 sec
						dataCenterInstance = hs.spawnInstance(ec2,	//spawn a new data center
								hs.dataCenterImgId, "m3.medium",	//instance
								loadGeneratorSubnet);
						
						//wait for it to become active
						hs.waitForInstanceRunningState(ec2, dataCenterInstance);
						
						//get its public DNS
						dataCenterDNS = hs.getInstancePublicDnsName(ec2,
								dataCenterInstance.getInstanceId());
						
						//Prepare to add this instance to Horizontal test
						String addDCUrl = "http://" + loadGenDNS
								+ "/test/horizontal/add?dns=" + dataCenterDNS;
						URL addInstance = new URL(addDCUrl);
						Thread.sleep(5000);		//buffer time for instance to be
												//ready before connecting to it
						HttpURLConnection con2 = (HttpURLConnection) addInstance
								.openConnection();
						con2.setRequestMethod("GET");
						con2.setDoOutput(true);
						con2.getResponseCode();
						sb.setLength(0);	//clear string builder
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
