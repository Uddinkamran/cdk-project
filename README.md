Common Ingestion Pipeline with AWS CDK

To enable data analytics and AI/ML there are multiple steps a business has to take to create the foundations necessary to utilize all the data across the company. The process is known as Extract, Transform and Load (ETL), which helps business take their data from all the different providers and perform cleaning. The cleaning phase allows the business to structure the data and contextualize the data for useful analytics such as predictive analysis. In this project, I will be creating a simplified architecture to perform these steps using infrastructure as code, AWS CDK. The AWS CDK is a new software development framework from AWS, which allows developers to build cloud infrastructure using their favorite programming language. AWS CDK support multiple languages, such as, JavaScript, TypeScript, Python, C#, and Java.

Extraction: 
Amazon Kinesis- Amazon Kinesis makes it easy to collect, process, and analyze real-time, streaming data so you can get timely insights and react quickly to new information 

Transform: 
Amazon Glue: AWS Glue is a serverless data integration service that makes it easier to discover, prepare, move, and integrate data from multiple sources for analytics, machine learning (ML), and application development.

Load: 
Amazon S3- Amazon Simple Storage Service (Amazon S3) is an object storage service offering industry-leading scalability, data availability, security, and performance. Customers of all sizes and industries can store and protect any amount of data for virtually any use case, such as data lakes, cloud-native applications, and mobile apps.

<img width="639" alt="Screen Shot 2023-01-17 at 1 00 02 PM" src="https://user-images.githubusercontent.com/33004407/212976433-46f21e55-ce21-4737-a53a-fc6f01e6da9e.png">

Architecture: In this project, AWS CDK is used to create Amazon Kinesis data stream, glue job and Amazon S3 to store the data. To further expand on the architecture, we would create Amazon S3 buckets for original data, clean data and long-term storage. We would use Amazon QuickSight for data visualization and intelligence by connecting to Amazon Athena, which enables interactive queries.!



