# Africa-Pulse

Decisions made:
On clickhouse, I created a clickhouse database then I would add my data from a clickhouse postgres instance

Before transferring the data, I would create a table in clickhouse with the same schema as the postgres table.
Then I would use the clickhouse-client to connect to the clickhouse database

## Data sources

weather : open-meteo

mobility : opensky

fx: ExchangeRate-API pro plan , They offer 2 weeks free trial

