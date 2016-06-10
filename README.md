### hackdays-beam

To compile a simple date dimension and run it:

```sh
mvn clean install -DskipTests
mvn exec:exec -Doutput=datetime.avro
```
