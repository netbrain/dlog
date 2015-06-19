/*
Package model contains the datamodel used in wire transfers.


The following is the binary representation of the different entities

	|---------------------------------------------------------------|
	| MetaData                                                      |
	| ClientID (64) | ClientMessageNumber (64) | TransactionId (64) |
	|---------------------------------------------------------------|
	| LogEntry                                                      |
	| MetaData | Payload (scalar)                                   |
	|---------------------------------------------------------------|
	| Request                                                       |
	| Type (1) | [LogEntry]                                         |
	|---------------------------------------------------------------|
*/
package model
