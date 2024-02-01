# domains-salesforce-to-dnswatch

Constructs to facilitate update of DNSWatch from Salesforce. An eventbridge scheduled task is set up which periodically calls a state machine.

The state machine has 2 steps:

The first calls salesforce for updated records on `Account` and `Domain Relation` and also for orphaned (mimicing the salesforce scraper). The json received from salesforce is written in to s3

The second step is designed to process the json by calling APIs into DNSWatch.

The stack also creates parameters and secrets (for salesforce access) and buckets etc
