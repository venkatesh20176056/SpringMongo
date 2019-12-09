# MAP Features Prototyping in Zeppelin

## Creating Zeppelin EMR
* Choose emr-5.19.0 with Zeppelin, Spark and Ganglia(for resource monitoring)
* Create a role for EC2 instance profile
* Ask Midgar and DaaS team to provide s3 access to the role 

## Setting up MAP-features in Zeppelin
* Follow map-features/README.md to package mapfeatures.jar
* Upload jar to zeppelin node (EMR master node)  
* Open zeppelin interpreter [zeppelin-host]:8890/#/interpreter  
* Add the following properties to the spark interpreter:
    * `spark.jars` -> `[path to jar]`
    * `spark.driver.extraJavaOptions` -> `-Dconfig.resource=prod_emr.conf`
    * `zeppelin.spark.useHiveContext` -> `false`
* Set up desired Spark session isolation mode: https://zeppelin.apache.org/docs/0.8.0/usage/interpreter/interpreter_binding_mode.html

## Tips for running Zeppelin notebook
* Keyboard shortcut
    * Auto-complete: option-space