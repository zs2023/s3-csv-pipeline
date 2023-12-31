Steps to run the test:
1. Open terminal
2. Pull the docker image using: docker pull zs2023/s3:s3
3. Run the interactive image using: docker run -it zs2023/s3:s3
4. After the container has been setup, run: python3 main.py --date 2021/01/30
5. The target csv file with aggregated data will be uploaded to the S3 bucket with path: global-interview-bucket/results/2021/01/30/daily_agg_20210130_ZS.csv

Remarks:
1. Use argparse to set up the parameters when running with bash, restrict the datetime format in '%Y/%m/%d' and make sure the input date is zero-padded. However, more exception cases can be added such as when there is no csv file under the path with correctly formatted input date. 
2. The process is idempotent with overwrite mode selected when writing the csv file.
3. Use coalesce(1) to combine the csv files into a single file as the dataframe is partitioned. 
4. When setup the spark session, using config to add ('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-common:3.3.4') to avoid error: 
	py4j.protocol.Py4JJavaError: An error occurred while calling o29.csv. 
	: java.lang.RuntimeException: java.lang.ClassNotFoundException: Class
	org.apache.hadoop.fs.s3a.S3AFileSystem not found
5. I use VSC to write the python code and run the code with WSL running ubuntu 22.04
6. In the Dockerfile, I have an issue using pip to install packages in requirements.txt. The error message is 
	Getting requirements to build wheel: finished with status 'error'
	15.60   error: subprocess-exited-with-error
	exit code: 1
	note: This error originates from a subprocess, and is likely not a problem with pip.
   I have no idea why this happened. Therefore, I have to write the pip3 install separately.
7. Credentials are stored separately in config.yaml and are called with pyyaml and write to the environment with os.environ.
   config.yaml file has not been uploaded. It is in the format as follow:
	aws:
	  access_key: "access_key"
	  secret_key: "secret_key"
