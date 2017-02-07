########################################### Almost fixed for all experiments ###################################################
no_tensor=2
tensor1_dim="7889_154"
tensor2_dim="62_154"

split=48
stopping="0_0.0001"

hdfs_dir="hdfs://orion12:9000/user/dudo/"
output_dir="/home/dudo/Experiments/CDRec/"

coupledMapPath="/home/dudo/SourceCode/CDRec/NSW_Crime_map.txt" 

input1="${hdfs_dir}train_NSW_10percent_SparseMatrix_7889_154.txt" 
input2="${hdfs_dir}train_NSW_Crime_Sparse_62_154.txt"
validation1="${hdfs_dir}validation_NSW_10percent_SparseMatrix_7889_154.txt"
validation2="${hdfs_dir}validation_NSW_Crime_Sparse_62_154.txt"
########################################### Change these parameters ###################################################
optimization="CF_0.1_LS"
rank=5

program_path="target/scala-2.10/cdrec_2.10-1.0.jar"
output="${output_dir}test_${optimization}_R${rank}#1"
spark-submit \
	--class "SparkCDRec" \
	--master local[*] \
	--driver-memory 10G \
	target/scala-2.10/cdrec_2.10-1.0.jar $no_tensor $tensor1_dim $tensor2_dim $split $rank $coupledMapPath $optimization $stopping $input1 $input2 $output $validation1 $validation2
