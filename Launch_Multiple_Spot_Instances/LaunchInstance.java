import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AvailabilityZone;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusResult;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsRequest;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStatus;
import com.amazonaws.services.ec2.model.InstanceStatusSummary;
import com.amazonaws.services.ec2.model.LaunchSpecification;
import com.amazonaws.services.ec2.model.RequestSpotInstancesRequest;
import com.amazonaws.services.ec2.model.RequestSpotInstancesResult;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;


public class LaunchInstance {
	private String propertyFile = "/AwsCredentials.properties";
	private Regions regions = Regions.US_EAST_1;
	private String endPoint = "ec2.us-east-1.amazonaws.com";
	private String tagKey = "Project";
	private String tagValue = "3.5";
	private String securityGroupId = null;
	private String securityGroupName = "default";
	
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
	
	public AmazonEC2Client instantiateEC2Client(BasicAWSCredentials credentials) {
		// Create an Amazon EC2 Client
		AmazonEC2Client ec2 = new AmazonEC2Client(credentials);
		// Set region and endpoint
		Region usEast1 = Region.getRegion(this.regions);
		ec2.setRegion(usEast1);
		ec2.setEndpoint(this.endPoint);
		return ec2;
	}
	
	public void spawnInstance(AmazonEC2Client ec2, String imageId,
			String instanceType, String spotPrice, String subnetId) {
		
		List<String> secGrpIds = new ArrayList<String>();
		secGrpIds.add(this.securityGroupId);
		
		// Create Instance Request
		RequestSpotInstancesRequest rsir = new RequestSpotInstancesRequest();
		rsir.withSpotPrice(spotPrice);
		
		LaunchSpecification ls = new LaunchSpecification();
		ls.setImageId(imageId);
		ls.setInstanceType(instanceType);
		//ls.withSecurityGroups(securityGroupsIds);
		ls.withKeyName("P1.1");
		ls.setSubnetId(subnetId);
		//ls.setSecurityGroups(secGrpIds);
		
		rsir.setLaunchSpecification(ls);

		RequestSpotInstancesResult rsires = ec2.requestSpotInstances(rsir);

	}
	
	public void findSecGrpId(AmazonEC2Client ec2) {
		DescribeSecurityGroupsRequest dsgr = new DescribeSecurityGroupsRequest();
		dsgr.withGroupNames(this.securityGroupName);

		DescribeSecurityGroupsResult dsgres = ec2.describeSecurityGroups(dsgr);
		if (dsgres.getSecurityGroups().size() > 0)
			this.securityGroupId = dsgres.getSecurityGroups().get(0)
					.getGroupId();
	}
	
	public void waitForInstanceRunningStateAndTag(AmazonEC2Client ec2) {
		ArrayList<String> instanceIds = new ArrayList<String>();
		DescribeInstanceStatusRequest dir = new DescribeInstanceStatusRequest();
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
		for(int i=0;i<state.size();i++){
			String instanceId = state.get(i).getInstanceId();
			instanceIds.add(instanceId);
		}
		ArrayList<Tag> requestTags = new ArrayList<Tag>();
		requestTags.add(new Tag(this.tagKey, this.tagValue));
		CreateTagsRequest createTagsRequest_requests = new CreateTagsRequest();
		createTagsRequest_requests.setResources(instanceIds);
		createTagsRequest_requests.setTags(requestTags);
		ec2.createTags(createTagsRequest_requests);
	}
	
	public static void main(String[] args) {
		BasicAWSCredentials credentials = null;
		AmazonEC2Client ec2 = null;
		
		LaunchInstance li = new LaunchInstance();
		credentials = li.readCredentials();
		
		ec2 = li.instantiateEC2Client(credentials);
		li.findSecGrpId(ec2);
		
		li.spawnInstance(ec2, "ami-84562dec", "m1.small", "0.008", "subnet-c99243e2");
		li.spawnInstance(ec2, "ami-84562dec", "m1.small", "0.008", "subnet-c99243e2");
		//li.spawnInstance(ec2, "ami-be6551d6", "t1.micro", "0.004", "subnet-c99243e2");
		//li.spawnInstance(ec2, "ami-de6155b6", "t1.micro", "0.004", "subnet-c99243e2");
		//li.spawnInstance(ec2, "ami-cc6155a4", "m1.small", "0.008", "subnet-c99243e2");
		//li.spawnInstance(ec2, "ami-407a5528", "m3.xlarge", "0.05", "subnet-c99243e2");
		
		li.waitForInstanceRunningStateAndTag(ec2);
	}

}
