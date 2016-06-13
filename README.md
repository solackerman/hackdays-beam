### hackdays-beam

This demo reads from the kafka topic `checkout`, and summing up the number of checkouts per shop per minute in real time. It allows for up to 2 hours lag in late arriving data, and when late data is found, it outputs the updated new value every minute. To save memory, it only looks at 3 shops.

```sh
mvn clean install -DskipTests && mvn exec:exec -Dparallelism=8
```
