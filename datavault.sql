create database smdvault;

create table "HubExperiment"(
"sequence" text not null unique,
"timestamp" time not null,
"source" text not null,
	PRIMARY KEY ("sequence","timestamp","source")
);

create table "HubTreatment"(
"sequence" text not null unique,
"timestamp" time not null,
"source" text not null,
"experiment" text not null REFERENCES "HubExperiment"("sequence"),
	PRIMARY KEY ("sequence","timestamp","source")
);

create table "HubFactor"(
"sequence" text not null unique,
"timestamp" time not null,
"source" text not null,
"experiment" text not null REFERENCES "HubExperiment"("sequence"),
"isCofactor" bool DEFAULT false,
	PRIMARY KEY("sequence","timestamp","source","experiment")
);

create table "SatFactorName"(
"sequence" text not null REFERENCES "HubFactor"("sequence"),
"timestamp" time not null,
"source" text not null,
"name" varchar(40),
	PRIMARY KEY("sequence","timestamp","source")
);

create table "SatFactorLevel"(
"sequence" text not null unique REFERENCES "HubFactor"("sequence"),
"timestamp" time not null,
"source" text not null,
"levelValue"  varchar(40),
	PRIMARY KEY("sequence","timestamp","source")
);

create table "SatTreatmentFactorLevel"(
"sequence" text not null REFERENCES "HubTreatment"("sequence"),
"timestamp" time not null,
"source" text not null,
"factorLevel"  text not null REFERENCES "SatFactorLevel"("sequence"),
	PRIMARY KEY("sequence","timestamp","source")
);

create table "SatExperimentTitle"(
"sequence" text not null REFERENCES "HubExperiment"("sequence"),
"timestamp" time not null,
"source" text not null,
"title"  varchar(255),
	PRIMARY KEY("sequence","timestamp","source")
);

create table "SatExperimentAcronym"(
"sequence" text not null REFERENCES "HubExperiment"("sequence"),
"timestamp" time not null,
"source" text not null,
"acronym"  varchar(15),
	PRIMARY KEY("sequence","timestamp","source")
);

create table "HubExperimentalUnit"(
"sequence" text not null unique ,
"timestamp" time not null,
"source" text not null,
	PRIMARY KEY("sequence","timestamp","source")
);

create table "ParticipatesIn"(
"sequence" text not null unique ,
"timestamp" time not null,
"source" text not null,
"experimentalUnit" text not null REFERENCES "HubExperimentalUnit"("sequence"),
"experiment" text not null REFERENCES "HubExperiment"("sequence"),
	PRIMARY KEY("sequence","timestamp","source")
);

create table "SatExperimentalUnitIdentifier"(
"sequence" text not null REFERENCES "ParticipatesIn"("sequence"),
"timestamp" time not null,
"source" text not null,
"ID" varchar(15),
	PRIMARY KEY("sequence","timestamp","source")
);

create table "HubSubject"(
"sequence" text not null unique REFERENCES "HubExperimentalUnit"("sequence"),
"timestamp" time not null,
"source" text not null,
"name" varchar(40),
	PRIMARY KEY("sequence","timestamp","source")
);

create table "SatSubjectAge"(
"sequence" text not null REFERENCES "HubSubject"("sequence"),
"timestamp" time not null,
"source" text not null,
"age" int,
	PRIMARY KEY("sequence","timestamp","source")
);

create table "SatSubjectName"(
"sequence" text not null REFERENCES "HubSubject"("sequence"),
"timestamp" time not null,
"source" text not null,
"name" varchar(40),
	PRIMARY KEY("sequence","timestamp","source")
);

create table "HubGroup"(
"sequence" text not null unique,
"timestamp" time not null,
"source" text not null,
"treatment" text not null REFERENCES "HubTreatment"("sequence"),
	PRIMARY KEY("sequence","timestamp","source")
);

create table "AssignedTo"(
"sequence" text not null,
"timestamp" time not null,
"source" text not null,
"experimentalUnit" text not null REFERENCES "HubExperimentalUnit"("sequence"),
"group" text not null REFERENCES "HubGroup"("sequence"),
	PRIMARY KEY("sequence","timestamp","source")
);

create table "SatGroupName"(
"sequence" text not null REFERENCES "HubGroup"("sequence"),
"timestamp" time not null,
"source" text not null,
"name" varchar(40),
	PRIMARY KEY("sequence","timestamp","source")
);

create table "HubSession"(
"sequence" text not null unique,
"timestamp" time not null,
"source" text not null,
	PRIMARY KEY("sequence","timestamp","source")
);

create table "SatSessionName"(
"sequence" text not null REFERENCES "HubSession"("sequence"),
"timestamp" time not null,
"source" text not null,
"name" varchar(40),
	PRIMARY KEY("sequence","timestamp","source")
);

create table "AttendsSession"(
"sequence" text not null,
"timestamp" time not null,
"source" text not null,
"experimentalUnit" text not null REFERENCES "HubExperimentalUnit"("sequence"),
"group" text not null REFERENCES "HubGroup"("sequence"),
"session" text not null REFERENCES "HubSession"("sequence"),
	PRIMARY KEY("sequence","timestamp","source")
);

create table "HubObservation"(
"sequence" text not null unique,
"timestamp" time not null,
"source" text not null,
"collectedAtSession" text not null REFERENCES "HubSession"("sequence"),
	PRIMARY KEY("sequence","timestamp","source")
);

create table "SatObservationName"(
"sequence" text not null REFERENCES "HubObservation"("sequence"),
"timestamp" time not null,
"source" text not null,
"name" varchar(40),
	PRIMARY KEY("sequence","timestamp","source")
);

create table "SatObservationValue"(
"sequence" text not null REFERENCES "HubObservation"("sequence"),
"timestamp" time not null,
"source" text not null,
"value" float(8)[][],
"timestamps" timestamp[],
	PRIMARY KEY("sequence","timestamp","source")
);

create table "HubMetaData"(
"sequence" text not null unique,
"timestamp" time not null,
"source" text not null,
	PRIMARY KEY("sequence","timestamp","source")
);

create table "ObservationMetaData"(
"sequence" text not null,
"timestamp" time not null,
"source" text not null,
"observation" text not null REFERENCES "HubObservation"("sequence"),
"metadata" text not null REFERENCES "HubMetaData"("sequence"),
	PRIMARY KEY("sequence","timestamp","source")
);

create table "SessionMetaData"(
"sequence" text not null,
"timestamp" time not null,
"source" text not null,
"session" text not null REFERENCES "HubSession"("sequence"),
"metadata" text not null REFERENCES "HubMetaData"("sequence"),
	PRIMARY KEY("sequence","timestamp","source")
);

create table "SatMetaDataKeyValuePair"(
"sequence" text not null REFERENCES "HubMetaData"("sequence"),
"timestamp" time not null,
"source" text not null,
"key" varchar(40) not null,
"value" bytea,
	PRIMARY KEY("sequence","timestamp","source")
);