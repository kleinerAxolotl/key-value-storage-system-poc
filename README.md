# Persistent Key Value Storage POC

The objective is to implement a network-available persistent Key/Value system that exposes the interfaces listed below in Java relying only on the standard libraries.

## Endpoints available for operations
### Store a Key-Value Pair
````
http://localhost:8080/put?key={key}}&value={value}
-response: Stored {key}

Example:
curl -X PUT "http://localhost:8080/put?key=day&value=Monday"
Stored day
````

### Retrieve a Value
````
http://localhost:8080/get?key={key}
-response: {value}

Example:
curl -X GET "http://localhost:8080/get?key=day"
Monday
````

### Store Multiple Keys
````
http://localhost:8080/putBatch
Payload:
{
    keys:["{key1}", "{key2}",... "{keyn}"],
    values:["{value1}", "{value2}",... "{valuen}"]
}
-response: Batch Stored

Example:
curl -X PUT -d '{keys:["John", "Fred","Raul"],values:["10", "20","30"]}' http://localhost:8080/putBatch
Batch Stored
````

### Retrieve by Key Range
````
http://localhost:8080/getRange?startKey={startKey}&endKey={endKey}
-response: {key1=val1, key2=val2, ...keyn=valn}

Example:
curl -X GET "http://localhost:8080/getRange?startKey=a&endKey=z"
{a=a, b=b, ...z=z}
````

### Delete a Key
````
http://localhost:8080/delete?key={key}
-response: Deleted {key}

Example:
curl -X DELETE "http://localhost:8080/delete?key=day"
Deleted day
````


## Steps to run the project:
### 1) compile the source code
````
javac src/main/java/com/task/keyvaluestorage/*.java src/main/java/com/task/keyvaluestorage/core/*.java src/main/java/com/task/keyvaluestorage/network/*.java -d classes/
````
Alternatives
````
Linux/macOS:
javac -d classes $(find src -name "*.java")

Windows cmd:
javac -d classes src\**\*.java

PowerShell:
javac -d classes (Get-ChildItem -Path src -Recurse -Filter "*.java").FullName
````

### 2) create a jar file
````
jar cvfe kvs.jar com.task.keyvaluestorage.KeyValueStorageServer -C classes .
````

### 3) run the jar file
````
java -jar kvs.jar
````
### Note:
#### Java Development Kit (JDK) 17 or higher version is required.

The JDK provides:
* **javac:** Java compiler
* **jar:** JAR file creator
* **java:** Java runtime
