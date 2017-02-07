# CDRec
Cross Domain Recommender

# Authors
Quan Do: https://sites.google.com/site/minhquandd/
* Feel free to contactme for any question or suggestion.

# Reference
Please reference as: Quan Do, Wei Liu and Fang Chen, Discovering both Explicit and Implicit Similarities for Cross-Domain Recommendation, in Proceedings of the 2017 Pacific-Asia Conference on Knowledge Discovery and Data Mining (PAKDD), 2017

    @inproceedings{Do_LiST17,
      author = {Do, Quan and Liu, Wei and Chen, Fang},
      title = {Discovering both Explicit and Implicit Similarities for Cross-Domain Recommendation},
      booktitle = {Proceedings of the 2017 Pacific-Asia Conference on Knowledge Discovery and Data Mining (PAKDD)},
      year = {2017},
    }

# Copyright
This software is free for research purposes. For commercial purposes, please contact the authors.

# File format
Input file is formatted with this rule:
  
	index_1 \t index_2 \t .... index_n \t value

For example, each observed entry of a 3x3 matrix is recorded in the input file:
  
	0 0 1
	0 1 2
 	...
	2 2 5

or each observed entry of a 3x3x3 tensor is recorded in the input file:
  
	0 0 0 1
	0 0 1 2
	...
  	2 2 2 5
  
# Usage
Parameters: [number of tensor] {[tensor_1's size] ... [tensor_n's size]} [split] [rank] [coupled_map filename] [optimizer] [stopping condition] {[tensor_1's input path] ... [tensor_n's input path]} [output_key] {[tensor_1's valid path] ... [tensor_n's valid path]}
	
	[number of tensor]: number of tensors joint decomposed
	[tensor's size]: mode1-length_mode2-length_..._modeN-length 
                    (e.g., 3D tensor of 1500x500x100 then [tensor's size] is 1500_500_100)
	[split]: number of distributed parts
	[rank]: rank of decomposition
	[coupled_map filename]: in case of more than 1 tensor are joint decomposed, this file must be used.
		It describes coupled relationship between them. 
		The format of this file is 
			[tensor1 index]_[mode index] [tensor2 index]_[mode index]. 
			(e.g., 1st mode of tensor 1 is coupled with 2nd mode of tensor 2. Suppose the index starts with 0, then the content of this file is 0_0 1_1)
	[optimizer]: either
		Closed form solution: CF_[RegParam]_[Gradient method]
			[Gradient method]: 
				Least square: LS
				Weighted least square: WLS
			[RegParam]: regularizer Coefficient (in case L1 or L2 is used)
	[stoping condition]: three different conditions
		small difference changes: 0_[min diff], e.g., 0_0.00001
		max iteration: 1_[max iteration], e.g., 1_100
		max hours: 2_[max hour]. e.g., 2_4
	[tensors' input path]: paths to input files in HDFS
	[output_key]: directory to local disk with prefix key of the output directory
	[tensors' valid path]: paths to validation files in HDFS (optional)

# Spark deployment mode:
Local mode
	
	spark-submit \
		--class "[class name]" \
		--master local[*] \
		--driver-memory 8G \
		[location of jar file] [Parameters]
Cluster mode
	
	spark-submit \
		--class "[class name]" \
		--master yarn \
		--deploy-mode cluster \
		--driver-memory 20G \
		--executor-memory 20G \
		--num-executors 45 \
		--executor-cores 14 \
		[location of jar file] [Parameters]

# Testing
The code was compiled with Scala 2.10.4 and extensively tested with Spark version 1.6.0 on Hadoop version 2.7.1.
For testing purpose, I put the compiled jar file in "compiled jar" folder and wrote two executable scripts: run_CTF.sh and run_TF.sh. Before executing any of them, please make sure HDFS and Hadoop Yarn have been started and sample data files have been located in the corresponding HDFS folder.
