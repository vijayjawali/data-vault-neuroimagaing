\connect smdvault;
CREATE VIEW "FactObservation" AS (
	select 
		omd."sequence" AS "ObservationMetaDataKey",
		ho."sequence" AS "ObservationKey",
		hmd."sequence" AS "MetaDataKey"
	from "ObservationMetaData" omd
	INNER JOIN "HubMetaData" hmd ON (
		hmd."sequence" = omd."metadata"
	)
	INNER JOIN "HubObservation" ho ON (
		ho."sequence" = omd."observation"
	)
);

CREATE VIEW "DimObservation" AS (
	select 
		ho."sequence" AS "ObservationKey",
		son."name",
		sov."value",
		sov."timestamps"
	from "HubObservation" ho
	INNER JOIN "SatObservationName" son ON (
		ho."sequence" = son."sequence"
	)
	INNER JOIN "SatObservationValue" sov ON (
		ho."sequence" = sov."sequence"
	)
);

CREATE VIEW "DimMetaData" AS(
	select 
		hmd."sequence" AS "MetaDataKey",
		smd."key",
		smd."value"
	from "HubMetaData" hmd
	INNER JOIN "SatMetaDataKeyValuePair" smd ON (
		hmd."sequence" = smd."sequence"
	)
);


CREATE VIEW "FactTreatmentFactors" AS (
	select 
		he."sequence" AS "Experimentkey",
		hf."sequence" AS "Factorkey",
		ht."sequence" AS "Treatmentkey",
		hf."isCofactor"
	from "HubExperiment" he
	INNER JOIN "HubFactor" hf ON (
		he."sequence" = hf."experiment"
	)
	INNER JOIN "HubTreatment" ht ON (
		he."sequence" = ht."sequence"
	)
);

CREATE VIEW "DimExperiment" AS (
	select 
		he."sequence" AS "ExperimentKey",
		st."title",
		sa."acronym"
	from "HubExperiment" he 
	INNER JOIN "SatExperimentTitle" st ON (
		he."sequence" = st."sequence"
	)
	INNER JOIN "SatExperimentAcronym" sa ON (
		he."sequence" = sa."sequence"
	)
);

CREATE VIEW "DimFactor" AS (
	select 
		hf."sequence" AS "FactorKey",
		sfn."name",
		sfl."levelValue"
	from "HubFactor" hf 
	INNER JOIN "SatFactorName" sfn ON (
		hf."sequence" = sfn."sequence"
	)
	INNER JOIN "SatFactorLevel" sfl ON (
		hf."sequence" = sfl."sequence"
	)
);

CREATE VIEW "DimTreatment" AS (
	select 
		ht."sequence" AS "TreatmentKey",
		sfl."levelValue"
	from "HubTreatment" ht 
	INNER JOIN "SatTreatmentFactorLevel" stfl ON (
		ht."sequence" = stfl."sequence"
	)
	INNER JOIN "SatFactorLevel" sfl ON (
		stfl."factorLevel" = sfl."sequence"
	)
);

CREATE VIEW "DimSession" AS (
	select 
		hs."sequence" AS "SessionKey",
		ssn."name"
	from "HubSession" hs 
	INNER JOIN "SatSessionName" ssn ON (
		hs."sequence" = ssn."sequence"
	)
);

CREATE VIEW "FactSessionMetaData" AS (
	select 
		smd."sequence" AS "SessionMetaDataKey",
		smd."session" AS "SessionKey",
		hmd."sequence" AS "MetaDataKey"
	from "SessionMetaData" smd
	INNER JOIN "HubSession" hs ON (
		hs."sequence" = smd."session"
	)
	INNER JOIN "HubMetaData" hmd ON (
		hmd."sequence" = smd."metadata"
	)
);

CREATE VIEW "DimGroup" AS (
	select 
		hg."sequence" AS "GroupKey",
		sgn."name"
	from "HubGroup" hg
	INNER JOIN "SatGroupName" sgn ON (
		hg."sequence" = sgn."sequence"
	)
);

CREATE VIEW "DimSubject" AS (
	select 
		hs."sequence" AS "SubjectKey",
		ssn."name",
		ssa."age"
	from "HubSubject" hs
	INNER JOIN "SatSubjectName" ssn ON (
		hs."sequence" = ssn."sequence"
	)
	INNER JOIN "SatSubjectAge" ssa ON (
		hs."sequence" = ssa."sequence"
	)
);

CREATE VIEW "FactAttendsSession" AS (
	select 
		ats."sequence" AS "AttendsSessionKey",
		heu."sequence" AS "ExperimentalUnitKey",
		hs."sequence" AS "SessionKey",
		hg."sequence" AS "GroupKey"
	from "AttendsSession" ats
	INNER JOIN "HubSession" hs ON (
		hs."sequence" = ats."session"
	)
	INNER JOIN "HubExperimentalUnit" heu ON (
		ats."experimentalUnit" = heu."sequence"
	)
	INNER JOIN "HubGroup" hg ON (
		hg."sequence" = ats."group"
	)
);

CREATE VIEW "FactAssignedTo" AS (
	select 
		ato."sequence" AS "AssignedToKey",
		heu."sequence" AS "ExperimentalUnitKey",
		hg."sequence" AS "GroupKey"
	from "AssignedTo" ato
	INNER JOIN "HubExperimentalUnit" heu ON (
		ato."experimentalUnit" = heu."sequence"
	)
	INNER JOIN "HubGroup" hg ON (
		hg."sequence" = ato."group"
	)
);

CREATE VIEW "FactParticipants" AS (
	select 
		pi."sequence" AS "ParticipatesInKey",
		heu."sequence" AS "ExperimentalUnitKey",
		he."sequence" AS "ExperimentKey",
		seui."ID"
	from "ParticipatesIn" pi
	INNER JOIN "HubExperimentalUnit" heu ON (
		pi."experimentalUnit" = heu."sequence"
	)
	INNER JOIN "HubExperiment" he ON (
		he."sequence" = pi."experiment"
	)
	INNER JOIN "SatExperimentalUnitIdentifier" seui ON (
		seui."sequence" = pi."sequence"
	)
);