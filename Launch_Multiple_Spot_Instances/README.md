This Java program can be used to launch multiple spot instances while tuning the following parameters for each:
- spot price
- AWS zone and subnet
- Instance type
- AWS Image ID
- Security Group
- Tags
The program waits for all instances to get into 'Ready' state and then exits.  