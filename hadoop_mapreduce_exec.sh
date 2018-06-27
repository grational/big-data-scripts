#!/usr/bin/env bash
set -euxo pipefail

usage() {
	code=${1}
	echo 'usage:'
	echo "${0##*/} <project_dir> [reducers=1]"
	exit $code
}
die() {
	emessage=$1
	ecode=$2
	echo 1>&2 "[ERROR] ${emessage}"
	exit $ecode
}
activate_container() {
	local container_name="${1}"
	docker run --name=$container_name --hostname=quickstart.cloudera --detach --interactive --tty --privileged -p 8088 cloudera/quickstart /bin/bash
	# subtask 2.1: fix the yarn configuration file to actually allow hadoop to run jobs
	local yarn_configuration_file='/etc/alternatives/hadoop-conf/yarn-site.xml'
	local yarn_conf_line="$(docker exec -it "${container_name}" grep -n '<configuration>' "${yarn_configuration_file}" | cut -d: -f1)" && ((yarn_conf_line++))
	local healthy_disk_properties='<property><name>yarn.nodemanager.disk-health-checker.min-healthy-disks</name><value>0.0</value></property> <property><name>yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage</name><value>100.0</value></property>'
	# modify the yarn configuration file
	docker exec -it "${container_name}" sed -i "${yarn_conf_line}i${healthy_disk_properties}" "${yarn_configuration_file}"
	# subtask 2.2: fix the docker-quickstart script to not exec bash
	# actually start the hadoop cluster
	docker exec -it "${container_name}" sudo service hadoop-hdfs-datanode start
	docker exec -it "${container_name}" sudo service hadoop-hdfs-journalnode start
	docker exec -it "${container_name}" sudo service hadoop-hdfs-namenode start
	docker exec -it "${container_name}" sudo service hadoop-hdfs-secondarynamenode start
	docker exec -it "${container_name}" sudo service hadoop-yarn-nodemanager start
	docker exec -it "${container_name}" sudo service hadoop-yarn-resourcemanager start
}

# process command line arguments
if (( ${#@} < 1 )); then usage 1; fi

[[ -d "${1}" ]] && { project_dir="${1}"; shift; }
(( ${#@} > 0 )) && [[ "${1}" =~ [1-9][0-9]* ]] && { reducers="${1}"; shift; } || reducers=1

# task 1: generate/refresh the jar file
# subtask 1.1: choose among gradle and maven (preferring the first one)
if [[ -f "${project_dir}/build.gradle" && -x "${project_dir}/gradlew" ]]; then
	bt=gradle
elif [[ -f "${project_dir}/pom.xml" && $(which mvn) ]]; then
	bt=maven
else
	die "no runnable configuration found for gradle or maven in '${project_dir}" 1
fi
# subtask 1.2: generate the jar
case $bt in
	gradle)
		(cd "${project_dir}" && ./gradlew clean jar)
		input_jar=$(ls -1t "${project_dir}"/build/libs/*.jar | head -n1)
		;;
	maven)
		(cd "${project_dir}" && sed -i -r 's/(source|target)>(1\.)?([89]|10)/\1>1.7/' pom.xm && mvn clean package)
		input_jar=$(ls -1t "${project_dir}"/target/*.jar | head -n1)
		;;
esac

# task 2: collect the container name running the clouder/quickstart container if needed
container_name='big_dingding'
if [[ $(docker ps -aq -f name="${container_name}") ]]; then
	if [[ $(docker ps -aq -f status=exited -f name="${container_name}") ]]; then
		docker start "${container_name}"
	elif ! [[ $(docker ps -aq -f status=running -f name="${container_name}") ]]; then
		docker rm "$container_name"
		activate_container "${container_name}"
	fi
else
	activate_container "${container_name}"
fi

# task 3: copy the input files into the container
input_data=$(ls -1d "${project_dir}"/*_data | head -n1)
case $bt in
	gradle)
		input_jar="$(ls -1t "${project_dir}"/build/libs/*.jar | head -n1)";;
	maven)
		input_jar="$(ls -1t "${project_dir}"/target/*.jar | head -n1)";;
esac
jar_name="${input_jar##*/}"
container_directory=/tmp
docker cp -a "${input_data}" "${container_name}:${container_directory}"
# copy the jar file
docker cp "${input_jar}" "${container_name}:${container_directory}"

# task 4: execute the commands needed to run the jar task on hadoop
# Remove folders of the previous run
input_directory=${input_data##*/}
output_directory="${input_directory}" && output_directory="${output_directory/_data/_out}"
docker exec -it "${container_name}" hdfs dfs -test -d "${input_directory}" && \
	docker exec -it "${container_name}" hdfs dfs -rm -r "${input_directory}"
docker exec -it "${container_name}" hdfs dfs -test -d "${output_directory}" && \
	docker exec -it "${container_name}" hdfs dfs -rm -r "${output_directory}"

# Put input data collection into hdfs
docker exec -it "${container_name}" hdfs dfs -put "${container_directory}/${input_directory}"

# Run application
main_classfile="$(grep -R main -l ${project_dir}/src | head -n1)"
d="${main_classfile}"
while ! [[ "$d" =~ ^src/main/java/ ]]; do d="${d#*/}"; done
main_class=$(echo ${d#*/*/*/} | cut -d. -f1 | tr / .)
if [[ $(ls -1 "${input_data}" | wc -l) -gt 1 ]]; then
	echo "select one of the following input files: "
	select input_file in $(ls -1 "${input_data}") "whole '${input_data}' directory"; do
		case $input_file in
			"whole '${input_data}' directory")
				input_file="${input_directory}"
				break ;;
			*)
				input_file="${input_directory}/${input_file}"
				break ;;
		esac
	done
else
	input_file="${input_directory}/$(ls -1 "${input_data}" | head -n1)"
fi
docker exec -it "${container_name}" hadoop jar "${container_directory}/${jar_name}" "${main_class}" ${reducers} "${input_file}" "${output_directory}" "${@}"

set +e
docker exec -it "${container_name}" bash -c "[[ -d '${container_directory}/${output_directory}' ]] && rm -rf '${container_directory}/${output_directory}'"
set -e
docker exec -it "${container_name}" hdfs dfs -get "${output_directory}" "${container_directory}"
docker exec -it "${container_name}" hdfs dfs -rm -r "${input_directory}"
docker exec -it "${container_name}" hdfs dfs -rm -r "${output_directory}"
set +e
[[ -d "${output_directory}" ]] && rm -rf "${output_directory}"
set -e
docker cp -a "${container_name}:${container_directory}/${output_directory}" .
echo "See the content of '${output_directory}'"

## open the hadoop web interface
[[ $(uname) == 'Darwin' ]] && command=open || command=xdg-open
hadoop_wui_port=$(docker port "${container_name}" | cut -d: -f2)
$command http://localhost:${hadoop_wui_port}/cluster 
